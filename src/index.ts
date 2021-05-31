import type { Operation } from "@apollo/client/link/core";
import type {
  OperationDefinitionNode,
  OperationTypeNode,
  DefinitionNode,
  DocumentNode,
  FieldNode,
  GraphQLSchema,
} from "graphql";
import { setContext } from "@apollo/client/link/context";
import { Kind, concatAST } from "graphql";

export { schemaBuilder } from "./schema-builder";

type OperationType = SchemaDefinition[OperationTypeNode];
type ContextBuilder<TContext = any> = (input: {
  modules: SchemaModule<TContext>[];
  operation: Operation;
}) => any;

/**
 * Represents the exported keywords of a module
 */
type SchemaModule<TContext = any> = EnsureProp<
  {
    typeDefs?: DocumentNode;
    resolvers?: any;
    context?(ctx: TContext): any;
  },
  "typeDefs",
  "resolvers"
>;

type EnsureProp<T extends object, K1 extends keyof T, K2 extends keyof T> = T &
  (Required<Pick<T, K1>> | Required<Pick<T, K2>> | Required<Pick<T, K1 | K2>>);

/**
 * A function that loads a module
 */
type SchemaModuleLoader = () => Promise<SchemaModule>;

interface Dependencies {
  [moduleIndex: string]: number[] | undefined;
}

type ModuleList = number[];

type SchemaModuleMapInternal = {
  preloadModules?: ModuleList;
  modules?: SchemaModuleLoader[];
  dependencies?: Dependencies;
  sharedModule: SchemaModuleLoader;
  types: {
    [typeName: string]: Record<string, number>;
  };
};

type Exact<T> = {
  [P in keyof T]: T[P];
};

/**
 * A map between fields of Queries, Mutations or Subscriptions and Schema Modules
 */
export type SchemaModuleMap = Exact<SchemaModuleMapInternal>;

interface SchemaDefinition {
  query: string;
  mutation: string;
  subscription: string;
}

export type IncrementalSchemaLinkOptions<TContext = any> = {
  map: SchemaModuleMap;
  schemaBuilder(input: {
    typeDefs: DocumentNode;
    resolvers: any[];
  }): GraphQLSchema;
  contextBuilder?: ContextBuilder<TContext>;
  resolvers?: any[];
  schemaDefinition?: SchemaDefinition;
  batchRequests?: Boolean;
};

export type WithIncremental<T extends any> = T & {
  incremental: {
    schema: GraphQLSchema;
    contextValue: any;
  };
};

/**
 * Creates an ApolloLink that lazy-loads parts of schema, with resolvers and context.
 */
export function createIncrementalSchemaLink<TContext = any>({
  map,
  resolvers,
  schemaBuilder,
  contextBuilder,
  schemaDefinition = {
    query: "Query",
    mutation: "Mutation",
    subscription: "Subscription",
  },
  batchRequests = false,
}: IncrementalSchemaLinkOptions<TContext>) {
  if (
    Object.values(schemaDefinition).length !== 3 ||
    Object.keys(schemaDefinition).filter(
      (key) => ["query", "mutation", "subscription"].includes(key) === false
    ).length !== 0
  ) {
    throw new Error(
      `"options.schemaDefinition" requires all 3 root types to be defined`
    );
  }

  const manager = SchemaModulesManager({
    map,
    resolvers,
    schemaBuilder,
    contextBuilder,
    schemaDefinition,
    batchRequests,
  });

  return setContext(async (op, prev) => {
    const { schema, contextValue } = await manager.prepare(op as any);

    return {
      ...prev,
      incremental: {
        schema,
        contextValue,
      },
    };
  });
}

function listDependencies(startAt: number[], map: SchemaModuleMap): number[] {
  if (!map.dependencies) {
    return startAt;
  }

  const visited: number[] = [];
  const maxId = map.modules?.length || 0;

  function visit(i: number | string) {
    const id = typeof i === "string" ? parseInt(i, 10) : i;

    if (id < 0 || id >= maxId) {
      return;
    }

    if (visited.indexOf(id) === -1) {
      visited.push(id);

      if (map.dependencies[id]?.length) {
        map.dependencies[id].forEach(visit);
      }
    }
  }

  startAt.forEach(visit);

  return visited;
}

/**
 * Manages Schema Module, orchestrates the lazy-loading, deals with schema building etc
 */
function SchemaModulesManager({
  map,
  resolvers = [],
  schemaBuilder,
  contextBuilder,
  schemaDefinition,
  batchRequests,
}: IncrementalSchemaLinkOptions) {
  let usedModules: number[] = map.preloadModules || [];

  /**
   * Collects a list of required modules (based on root-level fields)
   * and a kind of an operation (Q, M or S)
   */
  function collectRequiredModules(doc: DocumentNode): number[] {
    const [rootFields, operationKind] = findRootFieldsAndKind(
      doc,
      schemaDefinition
    );

    return listDependencies(
      rootFields
        .map((field) => map.types[operationKind]?.[field])
        .filter(onlyDefined)
        .filter(onlyUnique),
      map
    );
  }

  /**
   * Loads all requested modules by their id + shared module
   */
  async function loadModules(ids: number[]) {
    const mods = await Promise.all(ids.map((mod) => map.modules[mod]()));
    const shared = await map.sharedModule();

    return mods.concat([shared]);
  }

  /**
   * Builds GraphQLSchema object based on a list of module ids
   * Does the memoization internally to avoid unnecessary computations
   */
  async function _buildSchema(ids: number[]) {
    const modules = await loadModules(ids);

    // saves a list of used modules including those requested by operation
    usedModules = usedModules.concat(ids).filter(onlyUnique);

    const schema = schemaBuilder({
      typeDefs: concatAST(modules.map((m) => m.typeDefs).filter(onlyDefined)),
      resolvers: modules
        .map((m) => m.resolvers || {})
        .concat(...resolvers)
        .filter(onlyDefined),
    });

    return schema;
  }

  let buildSchema = subsetMemo(_buildSchema);
  if (batchRequests) {
    buildSchema = accumulateInImmediate(buildSchema);
  }

  async function prepare(
    operation: Operation
  ): Promise<{
    schema: GraphQLSchema;
    contextValue: any;
  }> {
    const modules = collectRequiredModules(operation.query);
    const modulesToLoad = modules.filter((mod) => !usedModules.includes(mod));
    const allModules = modulesToLoad.concat(usedModules);

    return {
      schema: await buildSchema(allModules),
      contextValue: contextBuilder
        ? contextBuilder({
            modules: await loadModules(allModules),
            operation,
          })
        : {},
    };
  }

  return {
    prepare,
  };
}

function findRootFieldsAndKind(
  doc: DocumentNode,
  schemaDefinition: SchemaDefinition
): [string[], OperationType] {
  const op = doc.definitions.find(isOperationNode)!;

  const rootFields = op.selectionSet.selections.map(
    (field) => (field as FieldNode).name.value
  );

  return [rootFields, schemaDefinition[op.operation]];
}

function isOperationNode(def: DefinitionNode): def is OperationDefinitionNode {
  return def.kind === Kind.OPERATION_DEFINITION;
}

function onlyUnique<T>(val: T, i: number, list: T[]) {
  return list.indexOf(val) === i;
}

// Memoization function that knows how to keep disjoint subsets of results in memory
// Assumes inputs are arrays of items
function subsetMemo<R>(fn: (input: number[]) => R) {
  const memoizedMap = new Map<string, R>();

  return (input: number[]): R => {
    const inputSet = input.slice().sort();
    const inputSetHash = inputSet.join("-");
    if (memoizedMap.has(inputSetHash)) {
      return memoizedMap.get(inputSetHash);
    } else {
      const result = fn(input);
      memoizedMap.set(inputSetHash, result);
      removeAllSubsets(memoizedMap, inputSet);
      return result;
    }
  };
}

function removeAllSubsets(map: Map<string, any>, set: number[]): void {
  for (let key of Array.from(map.keys())) {
    if (
      isSubsetOf(
        key.split("-").map((i) => Number.parseInt(i)),
        set
      )
    ) {
      map.delete(key);
    }
  }
}

// Strict subset function on sorted lists (excluding equal lists)
function isSubsetOf<A>(potentialSubset: A[], potentialParent: A[]): Boolean {
  if (potentialSubset.length >= potentialParent.length) {
    return false;
  }
  for (let i = 0; i < potentialSubset.length; i++) {
    if (potentialSubset[i] != potentialParent[i]) {
      return false;
    }
  }
  return true;
}

function onlyDefined<T>(val: T | undefined): val is T {
  return typeof val !== "undefined";
}

// This is partially copied from dataloader library

// MIT License

// Copyright (c) 2015-2018, Facebook, Inc.
// Copyright (c) 2019-present, GraphQL Foundation

// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:

// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.

// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

// Private: cached resolved Promise instance
let resolvedPromise;

// Private: Enqueue a Job to be executed after all "PromiseJobs" Jobs.
//
// ES6 JavaScript uses the concepts Job and JobQueue to schedule work to occur
// after the current execution context has completed:
// http://www.ecma-international.org/ecma-262/6.0/#sec-jobs-and-job-queues
//
// Node.js uses the `process.nextTick` mechanism to implement the concept of a
// Job, maintaining a global FIFO JobQueue for all Jobs, which is flushed after
// the current call stack ends.
//
// When calling `then` on a Promise, it enqueues a Job on a specific
// "PromiseJobs" JobQueue which is flushed in Node as a single Job on the
// global JobQueue.
//
// DataLoader batches all loads which occur in a single frame of execution, but
// should include in the batch all loads which occur during the flushing of the
// "PromiseJobs" JobQueue after that same execution frame.
//
// In order to avoid the DataLoader dispatch Job occuring before "PromiseJobs",
// A Promise Job is created with the sole purpose of enqueuing a global Job,
// ensuring that it always occurs after "PromiseJobs" ends.
//
// Node.js's job queue is unique. Browsers do not have an equivalent mechanism
// for enqueuing a job to be performed after promise microtasks and before the
// next macrotask. For browser environments, a macrotask is used (via
// setImmediate or setTimeout) at a potential performance penalty.
const enqueuePostPromiseJob =
  typeof process === "object" && typeof process.nextTick === "function"
    ? function (fn) {
        if (!resolvedPromise) {
          resolvedPromise = Promise.resolve();
        }
        resolvedPromise.then(() => {
          process.nextTick(fn);
        });
      }
    : setImmediate || setTimeout;

type Batch<R> = {
  hasDispatched: Boolean;
  promise: Promise<R>;
};

function accumulateInImmediate<A, R>(
  fn: (input: A[]) => Promise<R>
): (input: A[]) => Promise<R> {
  let accumulatedArgs: A[] = [];
  let batch: Batch<R> | undefined;
  return (args: A[]) => {
    accumulatedArgs.push(...args);
    if (!batch || batch.hasDispatched) {
      const promise: Promise<R> = new Promise((resolve) => {
        enqueuePostPromiseJob(() => {
          const fullArgs = accumulatedArgs;
          accumulatedArgs = [];
          batch.hasDispatched = true;
          resolve(fn(fullArgs));
        });
      });
      batch = {
        hasDispatched: false,
        promise,
      };
    }
    return batch.promise;
  };
}
