// This file is a part of Julia. License is MIT: https://julialang.org/license

// TODO:
// - coroutine integration. partr uses:
//   - ctx_construct(): establish the context for a coroutine, with an entry
//     point (partr_coro()), a stack, and a user data pointer (which is the
//     task pointer).
//   - ctx_get_user_ptr(): get the user data pointer (the task pointer).
//   - ctx_is_done(): has the coroutine ended?
//   - resume(): starts/resumes the coroutine specified by the passed context.
//   - yield()/yield_value(): causes the calling coroutine to yield back to
//     where it was resume()d.
// - stack management. pool of stacks to be implemented.
// - original task functionality to be integrated.
//   - world_age should go into task local storage?
//   - current_module is being obsoleted?

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <strings.h>

#include "julia.h"
#include "julia_internal.h"
#include "threading.h"

#ifdef __cplusplus
extern "C" {
#endif

#ifdef JULIA_ENABLE_THREADING
#ifdef JULIA_ENABLE_PARTR


// multiq
// ---

/* a task heap */
typedef struct taskheap_tag {
    jl_mutex_t lock;
    jl_ptask_t **tasks;
    int16_t ntasks, prio;
} taskheap_t;

static const int16_t heap_d = 8;
static const int heap_c = 4;
static const int tasks_per_heap = 129;

static taskheap_t *heaps;
static int16_t heap_p;

/* unbias state for the RNG */
static uint64_t cong_unbias;


/*  multiq_init()
 */
static inline void multiq_init()
{
    heap_p = heap_c * jl_n_threads;
    heaps = (taskheap_t *)calloc(heap_p, sizeof(taskheap_t));
    for (int16_t i = 0;  i < heap_p;  ++i) {
        jl_mutex_init(&heaps[i].lock);
        heaps[i].tasks = (jl_ptask_t **)calloc(tasks_per_heap, sizeof(jl_ptask_t *));
        heaps[i].ntasks = 0;
        heaps[i].prio = INT16_MAX;
    }
    unbias_cong(heap_p, &cong_unbias);
}


/*  sift_up()
 */
static inline void sift_up(taskheap_t *heap, int16_t idx)
{
    if (idx > 0) {
        int16_t parent = (idx-1)/heap_d;
        if (heap->tasks[idx]->prio <= heap->tasks[parent]->prio) {
            jl_ptask_t *t = heap->tasks[parent];
            heap->tasks[parent] = heap->tasks[idx];
            heap->tasks[idx] = t;
            sift_up(heap, parent);
        }
    }
}


/*  sift_down()
 */
static inline void sift_down(taskheap_t *heap, int16_t idx)
{
    if (idx < heap->ntasks) {
        for (int16_t child = heap_d*idx + 1;
                child < tasks_per_heap && child <= heap_d*idx + heap_d;
                ++child) {
            if (heap->tasks[child]
                    &&  heap->tasks[child]->prio <= heap->tasks[idx]->prio) {
                jl_ptask_t *t = heap->tasks[idx];
                heap->tasks[idx] = heap->tasks[child];
                heap->tasks[child] = t;
                sift_down(heap, child);
            }
        }
    }
}


/*  multiq_insert()
 */
static inline int multiq_insert(jl_ptask_t *task, int16_t priority)
{
    jl_ptls_t ptls = jl_get_ptls_states();
    uint64_t rn;

    task->prio = priority;
    do {
        rn = cong(heap_p, cong_unbias, &ptls->rngseed);
    } while (!jl_mutex_trylock_nogc(&heaps[rn].lock));

    if (heaps[rn].ntasks >= tasks_per_heap) {
        jl_mutex_unlock_nogc(&heaps[rn].lock);
        return -1;
    }

    heaps[rn].tasks[heaps[rn].ntasks++] = task;
    sift_up(&heaps[rn], heaps[rn].ntasks-1);
    jl_mutex_unlock_nogc(&heaps[rn].lock);
    int16_t prio = jl_atomic_load(&heaps[rn].prio);
    if (task->prio < prio)
        jl_atomic_compare_exchange(&heaps[rn].prio, prio, task->prio);

    return 0;
}


/*  multiq_deletemin()
 */
static inline jl_ptask_t *multiq_deletemin()
{
    jl_ptls_t ptls = jl_get_ptls_states();
    uint64_t rn1, rn2;
    int16_t i, prio1, prio2;
    jl_ptask_t *task;

    for (i = 0;  i < jl_n_threads;  ++i) {
        rn1 = cong(heap_p, cong_unbias, &ptls->rngseed);
        rn2 = cong(heap_p, cong_unbias, &ptls->rngseed);
        prio1 = jl_atomic_load(&heaps[rn1].prio);
        prio2 = jl_atomic_load(&heaps[rn2].prio);
        if (prio1 > prio2) {
            prio1 = prio2;
            rn1 = rn2;
        }
        else if (prio1 == prio2 && prio1 == INT16_MAX)
            continue;
        if (jl_mutex_trylock_nogc(&heaps[rn1].lock)) {
            if (prio1 == heaps[rn1].prio)
                break;
            jl_mutex_unlock_nogc(&heaps[rn1].lock);
        }
    }
    if (i == jl_n_threads)
        return NULL;

    task = heaps[rn1].tasks[0];
    heaps[rn1].tasks[0] = heaps[rn1].tasks[--heaps[rn1].ntasks];
    heaps[rn1].tasks[heaps[rn1].ntasks] = NULL;
    prio1 = INT16_MAX;
    if (heaps[rn1].ntasks > 0) {
        sift_down(&heaps[rn1], 0);
        prio1 = heaps[rn1].tasks[0]->prio;
    }
    jl_atomic_store(&heaps[rn1].prio, prio1);
    jl_mutex_unlock_nogc(&heaps[rn1].lock);

    return task;
}


// sync trees
// ---

/* arrival tree */
struct _arriver_t {
    int16_t index, next_avail;
    int16_t **tree;
};

/* reduction tree */
struct _reducer_t {
    int16_t index, next_avail;
    void ***tree;
};


/* pool of arrival trees */
static arriver_t *arriverpool;
static int16_t num_arrivers, num_arriver_tree_nodes, next_arriver;

/* pool of reduction trees */
static reducer_t *reducerpool;
static int16_t num_reducers, num_reducer_tree_nodes, next_reducer;


/*  synctreepool_init()
 */
static inline void synctreepool_init()
{
    num_arriver_tree_nodes = (GRAIN_K * jl_n_threads) - 1;
    num_reducer_tree_nodes = (2 * GRAIN_K * jl_n_threads) - 1;

    /* num_arrivers = ((GRAIN_K * jl_n_threads) ^ ARRIVERS_P) + 1 */
    num_arrivers = GRAIN_K * jl_n_threads;
    for (int i = 1;  i < ARRIVERS_P;  ++i)
        num_arrivers = num_arrivers * num_arrivers;
    ++num_arrivers;

    num_reducers = num_arrivers * REDUCERS_FRAC;

    /* allocate */
    arriverpool = (arriver_t *)calloc(num_arrivers, sizeof (arriver_t));
    next_arriver = 0;
    for (int i = 0;  i < num_arrivers;  ++i) {
        arriverpool[i].index = i;
        arriverpool[i].next_avail = i + 1;
        arriverpool[i].tree = (int16_t **)
                jl_malloc_aligned(num_arriver_tree_nodes * sizeof (int16_t *), 64);
        for (int j = 0;  j < num_arriver_tree_nodes;  ++j)
            arriverpool[i].tree[j] = (int16_t *)jl_malloc_aligned(sizeof (int16_t), 64);
    }
    arriverpool[num_arrivers - 1].next_avail = -1;

    reducerpool = (reducer_t *)calloc(num_reducers, sizeof (reducer_t));
    next_reducer = 0;
    for (int i = 0;  i < num_reducers;  ++i) {
        reducerpool[i].index = i;
        reducerpool[i].next_avail = i + 1;
        reducerpool[i].tree = (void ***)
                jl_malloc_aligned(num_reducer_tree_nodes * sizeof (void **), 64);
        for (int j = 0;  j < num_reducer_tree_nodes;  ++j)
            reducerpool[i].tree[j] = (void **)jl_malloc_aligned(sizeof (void *), 64);
    }
    if (num_reducers > 0)
        reducerpool[num_reducers - 1].next_avail = -1;
    else
        next_reducer = -1;
}


/*  arriver_alloc()
 */
static inline arriver_t *arriver_alloc()
{
    int16_t candidate;
    arriver_t *arr;

    do {
        candidate = jl_atomic_load(&next_arriver);
        if (candidate == -1)
            return NULL;
        arr = &arriverpool[candidate];
    } while (!jl_atomic_bool_compare_exchange(&next_arriver,
                candidate, arr->next_avail));
    return arr;
}


/*  arriver_free()
 */
static inline void arriver_free(arriver_t *arr)
{
    for (int i = 0;  i < num_arriver_tree_nodes;  ++i)
        *arr->tree[i] = 0;

    jl_atomic_exchange_generic(&next_arriver, &arr->index, &arr->next_avail);
}


/*  reducer_alloc()
 */
static inline reducer_t *reducer_alloc()
{
    int16_t candidate;
    reducer_t *red;

    do {
        candidate = jl_atomic_load(&next_reducer);
        if (candidate == -1)
            return NULL;
        red = &reducerpool[candidate];
    } while (!jl_atomic_bool_compare_exchange(&next_reducer,
                     candidate, red->next_avail));
    return red;
}


/*  reducer_free()
 */
static inline void reducer_free(reducer_t *red)
{
    for (int i = 0;  i < num_reducer_tree_nodes;  ++i)
        *red->tree[i] = 0;

    jl_atomic_exchange_generic(&next_reducer, &red->index, &red->next_avail);
}


/*  last_arriver()
 */
static inline int last_arriver(arriver_t *arr, int idx)
{
    int arrived, aidx = idx + (GRAIN_K * jl_n_threads) - 1;

    while (aidx > 0) {
        --aidx;
        aidx >>= 1;
        arrived = jl_atomic_fetch_add(arr->tree[aidx], 1);
        if (!arrived) return 0;
    }

    return 1;
}


/*  reduce()
 */
static inline void *reduce(arriver_t *arr, reducer_t *red, void *(*rf)(void *, void *),
                           void *val, int idx)
{
    int arrived, aidx = idx + (GRAIN_K * jl_n_threads) - 1, ridx = aidx, nidx;

    *red->tree[ridx] = val;
    while (aidx > 0) {
        --aidx;
        aidx >>= 1;
        arrived = jl_atomic_fetch_add(arr->tree[aidx], 1);
        if (!arrived) return NULL;

        /* neighbor has already arrived, get its value and reduce it */
        nidx = ridx & 0x1 ? ridx + 1 : ridx - 1;
        val = rf(val, *red->tree[nidx]);

        /* move up the tree */
        --ridx;
        ridx >>= 1;
        *red->tree[ridx] = val;
    }

    return val;
}


// parallel task runtime
// ---

// sticky task queues need to be visible to all threads
jl_ptaskq_t *sticky_taskqs;

// internally used to indicate a yield occurred in the runtime itself
// TODO: what's the Julia way to do this? A symbol?
static const int64_t yield_from_sync = 1;


// initialize the threading infrastructure
void jl_init_threadinginfra(void)
{
    /* initialize the synchronization trees pool and the multiqueue */
    synctreepool_init();
    multiq_init();

    /* allocate sticky task queues */
    sticky_taskqs = (jl_ptaskq_t *)jl_malloc_aligned(jl_n_threads * sizeof(jl_ptaskq_t), 64);
}


// initialize the thread function argument
void jl_init_threadarg(jl_threadarg_t *targ) { }


// helper for final thread initialization
static void init_started_thread()
{
    jl_ptls_t ptls = jl_get_ptls_states();

    /* allocate this thread's sticky task queue pointer and initialize the lock */
    seed_cong(&ptls->rngseed);
    ptls->sticky_taskq = &sticky_taskqs[ptls->tid];
    ptls->sticky_taskq->head = NULL;
}


// once the threads are started, perform any final initializations
void jl_init_started_threads(jl_threadarg_t **targs)
{
    // master thread final initialization
    init_started_thread();
}


static int run_next();


// thread function: used by all except the main thread
void jl_threadfun(void *arg)
{
    jl_threadarg_t *targ = (jl_threadarg_t *)arg;

    // initialize this thread (set tid, create heap, etc.)
    jl_init_threadtls(targ->tid);
    jl_init_stack_limits(0);

    jl_ptls_t ptls = jl_get_ptls_states();

    // set up tasking
    jl_init_root_task(ptls->stack_lo, ptls->stack_hi - ptls->stack_lo);
#ifdef COPY_STACKS
    jl_set_base_ctx((char*)&arg);
#endif

    init_started_thread();

    // Assuming the functions called below don't contain unprotected GC
    // critical region. In general, the following part of this function
    // shouldn't call any managed code without calling `jl_gc_unsafe_enter`
    // first.
    jl_gc_state_set(ptls, JL_GC_STATE_SAFE, 0);
    uv_barrier_wait(targ->barrier);

    // free the thread argument here
    free(targ);

    /* get the highest priority task and run it */
    while (run_next() == 0)
        ;
}


// coroutine entry point
static void partr_coro(void *ctx)
{
    jl_ptask_t *task = (jl_ptask_t *)ctx; // TODO. ctx_get_user_ptr(ctx);
    //task->result = task->f(task->arg, task->start, task->end);

    /* grain tasks must synchronize */
    if (task->grain_num >= 0) {
        int was_last = 0;

        /* reduce... */
        if (task->red) {
            // TODO. task->result = reduce(task->arr, task->red, task->rf,
            //                             task->result, task->grain_num);
            /*  if this task is last, set the result in the parent task */
            if (task->result) {
                task->parent->red_result = task->result;
                was_last = 1;
            }
        }
        /* ... or just sync */
        else {
            if (last_arriver(task->arr, task->grain_num))
                was_last = 1;
        }

        /* the last task to finish needs to finish up the loop */
        if (was_last) {
            /* a non-parent task must wake up the parent */
            if (task->grain_num > 0) {
                multiq_insert(task->parent, 0);
            }
            /* the parent task was last; it can just end */
            // TODO: free arriver/reducer when task completes
        }
        else {
            /* the parent task needs to wait */
            if (task->grain_num == 0) {
                // TODO. yield_value(task->ctx, (void *)yield_from_sync);
            }
        }
    }
}


// add the specified task to the sticky task queue
static void add_to_stickyq(jl_ptask_t *task)
{
    assert(task->sticky_tid != -1);

    jl_ptaskq_t *q = &sticky_taskqs[task->sticky_tid];
    JL_LOCK(&q->lock);
    if (q->head == NULL)
        q->head = task;
    else {
        jl_ptask_t *pt = q->head;
        while (pt->next)
            pt = pt->next;
        pt->next = task;
    }
    JL_UNLOCK(&q->lock);
}


// pop the first task off the sticky task queue
static jl_ptask_t *get_from_stickyq()
{
    jl_ptls_t ptls = jl_get_ptls_states();
    jl_ptaskq_t *q = ptls->sticky_taskq;

    /* racy check for quick path */
    if (q->head == NULL)
        return NULL;

    JL_LOCK(&q->lock);
    jl_ptask_t *task = q->head;
    if (task) {
        q->head = task->next;
        task->next = NULL;
    }
    JL_UNLOCK(&q->lock);

    return task;
}


// start the task if it is new, or switch to it
static jl_value_t *resume(jl_ptask_t *task)
{
    jl_ptls_t ptls = jl_get_ptls_states();

    // GC safe
    uint32_t nargs;
    jl_value_t **args;
    if (!jl_is_svec(task->args)) {
        nargs = 1;
        args = &task->args;
    }
    else {
        nargs = jl_svec_len(task->args);
        args = jl_svec_data(task->args);
    }

    // TODO: before we support getting return value from
    //       the work, and after we have proper GC transition
    //       support in the codegen and runtime we don't need to
    //       enter GC unsafe region when starting the work.
    int8_t gc_state = jl_gc_unsafe_enter(ptls);

    jl_value_t *result = NULL;
    if (!task->started) {
        task->started = 1;
        result = task->result = jl_thread_run_fun(&task->fptr, task->mfunc, args, nargs);
    }
    else {
        // TODO: switch to task
    }

    jl_gc_unsafe_leave(ptls, gc_state);

    return result;
}


// get the next available task and run it
static int run_next()
{
    jl_ptls_t ptls = jl_get_ptls_states();

    /* first check for sticky tasks */
    jl_ptask_t *task = get_from_stickyq();

    /* no sticky tasks, go to the multiq */
    if (task == NULL) {
        task = multiq_deletemin();
        if (task == NULL)
            return 0;

        /* a sticky task will only come out of the multiq if it has not been run */
        if (task->settings & TASK_IS_STICKY) {
            assert(task->sticky_tid == -1);
            task->sticky_tid = ptls->tid;
        }
    }

    /* run/resume the task */
    ptls->curr_task = task;

    // TODO
    int64_t y = 0;
    // int64_t y = (int64_t)resume(task->ctx);
    ptls->curr_task = NULL;

    /* if the task isn't done, it is either in a CQ, or must be re-queued */
    if (0 /* TODO. !ctx_is_done(task->ctx) */) {
        /* the yield value tells us if the task is in a CQ */
        if (y != yield_from_sync) {
            /* sticky tasks go to the thread's sticky queue */
            if (task->settings & TASK_IS_STICKY)
                add_to_stickyq(task);
            /* all others go back into the multiq */
            else
                multiq_insert(task, task->prio);
        }
        return 0;
    }

    /* The task completed. Detached tasks cannot be synced, so nothing will
       be in their CQs.
     */
    if (task->settings & TASK_IS_DETACHED)
        return 0;

    /* add back all the tasks in this one's completion queue */
    JL_LOCK(&task->cq.lock);
    jl_ptask_t *cqtask = task->cq.head;
    task->cq.head = NULL;
    JL_UNLOCK(&task->cq.lock);

    jl_ptask_t *cqnext;
    while (cqtask) {
        cqnext = cqtask->next;
        cqtask->next = NULL;
        if (cqtask->settings & TASK_IS_STICKY)
            add_to_stickyq(cqtask);
        else
            multiq_insert(cqtask, cqtask->prio);
        cqtask = cqnext;
    }

    return 0;
}


// specialize and compile the user function
static int setup_task_fun(jl_value_t *_args, jl_method_instance_t **mfunc,
                          jl_generic_fptr_t *fptr)
{
    jl_ptls_t ptls = jl_get_ptls_states();

    uint32_t nargs;
    jl_value_t **args;
    if (!jl_is_svec(_args)) {
        nargs = 1;
        args = &_args;
    }
    else {
        nargs = jl_svec_len(_args);
        args = jl_svec_data(_args);
    }

    *mfunc = jl_lookup_generic(args, nargs,
                               jl_int32hash_fast(jl_return_address()), ptls->world_age);

    // Ignore constant return value for now.
    if (jl_compile_method_internal(fptr, *mfunc))
        return 0;

    return 1;
}


// allocate and initialize a task
static jl_ptask_t *new_task(jl_value_t *_args)
{
    jl_ptls_t ptls = jl_get_ptls_states();

    // TODO: using jl_task_type below, assuming task and ptask will be merged
    jl_ptask_t *task = (jl_ptask_t *)jl_gc_alloc(ptls, sizeof (jl_ptask_t),
                                                 jl_task_type);
    // TODO. ctx_construct(task->ctx, task->stack, TASK_STACK_SIZE, partr_coro, task);
    if (!setup_task_fun(_args, &task->mfunc, &task->fptr))
        return NULL;
    task->args = _args;
    task->result = jl_nothing;
    task->current_module = ptls->current_module;
    task->world_age = ptls->world_age;
    task->sticky_tid = -1;
    task->grain_num = -1;

    return task;
}


// allocate a task and copy the specified task's contents into it
static jl_ptask_t *copy_task(jl_ptask_t *ft)
{
    jl_ptls_t ptls = jl_get_ptls_states();

    // TODO: using jl_task_type below, assuming task and ptask will be merged
    jl_ptask_t *task = (jl_ptask_t *)jl_gc_alloc(ptls, sizeof (jl_ptask_t),
                                                 jl_task_type);
    memcpy(task, ft, sizeof (jl_ptask_t));
    return task;
}


/*  partr_spawn() -- create a task for `f(arg)` and enqueue it for execution

    Implicitly asserts that `f(arg)` can run concurrently with everything
    else that's currently running. If `detach` is set, the spawned task
    will not be returned (and cannot be synced). Yields.
 */
int partr_spawn(partr_t *t, jl_value_t *_args, int8_t sticky, int8_t detach)
{
    jl_ptls_t ptls = jl_get_ptls_states();

    jl_ptask_t *task = new_task(_args);
    if (task == NULL)
        return -1;
    if (sticky)
        task->settings |= TASK_IS_STICKY;
    if (detach)
        task->settings |= TASK_IS_DETACHED;

    if (multiq_insert(task, ptls->tid) != 0) {
        return -2;
    }

    *t = detach ? NULL : (partr_t)task;

    /* only yield if we're running a non-sticky task */
    if (!(ptls->curr_task->settings & TASK_IS_STICKY))
        // TODO. yield(ptls->curr_task->ctx);
        ;

    return 0;
}


/*  partr_sync() -- get the return value of task `t`

    Returns only when task `t` has completed.
 */
int partr_sync(void **r, partr_t t)
{
    jl_ptask_t *task = (jl_ptask_t *)t;

    jl_ptls_t ptls = jl_get_ptls_states();

    /* if the target task has not finished, add the current task to its
       completion queue; the thread that runs the target task will add
       this task back to the ready queue
     */
    if (0 /* TODO. !ctx_is_done(task->ctx) */) {
        ptls->curr_task->next = NULL;
        JL_LOCK(&task->cq.lock);

        /* ensure the task didn't finish before we got the lock */
        if (0 /* TODO. !ctx_is_done(task->ctx) */) {
            /* add the current task to the CQ */
            if (task->cq.head == NULL)
                task->cq.head = ptls->curr_task;
            else {
                jl_ptask_t *pt = task->cq.head;
                while (pt->next)
                    pt = pt->next;
                pt->next = ptls->curr_task;
            }

            JL_UNLOCK(&task->cq.lock);
            /* yield point */
            // TODO. yield_value(ptls->curr_task->ctx, (void *)yield_from_sync);
        }

        /* the task finished before we could add to its CQ */
        else
            JL_UNLOCK(&task->cq.lock);
    }

    if (r)
        *r = task->grain_num >= 0 && task->red ?
                task->red_result : task->result;
    return 0;
}


/*  partr_parfor() -- spawn multiple tasks for a parallel loop

    Spawn tasks that invoke `f(arg, start, end)` such that the sum of `end-start`
    for all tasks is `count`. Uses `rf()`, if provided, to reduce the return
    values from the tasks, and returns the result. Yields.
 */
int partr_parfor(partr_t *t, jl_value_t *_args, int64_t count, jl_value_t *_rargs)
{
    jl_ptls_t ptls = jl_get_ptls_states();

    int64_t n = GRAIN_K * jl_n_threads;
    lldiv_t each = lldiv(count, n);

    /* allocate synchronization tree(s) */
    arriver_t *arr = arriver_alloc();
    if (arr == NULL)
        return -1;
    reducer_t *red = NULL;
    jl_method_instance_t *mredfunc;
    jl_generic_fptr_t rfptr;
    if (_rargs != NULL) {
        red = reducer_alloc();
        if (red == NULL) {
            arriver_free(arr);
            return -2;
        }
        if (!setup_task_fun(_rargs, &mredfunc, &rfptr)) {
            reducer_free(red);
            arriver_free(arr);
            return -3;
        }
    }

    /* allocate and enqueue (GRAIN_K * nthreads) tasks */
    *t = NULL;
    int64_t start = 0, end;
    for (int64_t i = 0;  i < n;  ++i) {
        end = start + each.quot + (i < each.rem ? 1 : 0);
        jl_ptask_t *task;
        if (*t == NULL)
            *t = task = new_task(_args);
        else
            task = copy_task(*t);
        if (task == NULL)
            return -4;

        task->start = start;
        task->end = end;
        task->parent = *t;
        task->grain_num = i;
        task->mredfunc = mredfunc;
        task->rfptr = rfptr;
        task->rargs = _rargs;
        task->arr = arr;
        task->red = red;

        if (multiq_insert(task, ptls->tid) != 0) {
            return -5;
        }

        start = end;
    }

    /* only yield if we're running a non-sticky task */
    if (!(ptls->curr_task->settings & TASK_IS_STICKY))
        // TODO. yield(curr_task->ctx);
        ;

    return 0;
}


#endif // JULIA_ENABLE_PARTR
#endif // JULIA_ENABLE_THREADING

#ifdef __cplusplus
}
#endif
