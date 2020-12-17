import multiprocessing as mp
import time
import functools
import operator
from collections import namedtuple
import queue

# User sends request in the form of an _expression_ in code or a data structure equivalent to code.
# E.g. f( g(), (h( i(), j(), const(2) ) ).
# Must be composed entirely of pure functions or functions which rely only on immutable external data.
# So results can be cached.

# Receiving service translates it into something that is added to the Ready queue.
#     (Said something has an identifier corresponding to the code.)
#     Then waits for a result _somehow_.
#     Then returns result to the user.
#     Might be better to return some key and let the user subscribe to
#     notifications of completed computations? Then  have a second service
#     for retrieving results given the key.
#

# A ready-queue worker picks it up from the Ready queue.
#     ? Can we work in affinity here?
#     If it is already in the cache:
#         The worker returns the value from the cache.
#     If it is a call w/ no arguments:
#         The worker executes it;
#         writes the result to the Results DB;
#         adds a Done message to the Done queue;
#         if the call was from the receiving service, signals the receiver that it was done;
#         removes the item from the Ready queue.
#     If the call has arguments which are unevaluated nested function calls:
#         The worker adds evaluations of the nested functions to the Ready queue;
#         adds an item to the Waiting queue/map for evaluating the call w/ evaluated nested arge to the Waiting queue;
#         removes the item from the Ready queue.
#     If the call has arguments and all have been evaluated:
#         The worker retrieves all the evaluated value from the Results DB;
#         executes the call;
#         writes the result to the Results DB;
#         adds a Done message to the Done queue;
#         if the call was from the receiving service, signals the receiver that it was done;
#         removes the item from the Ready queue.

# Recursively we will eventually get down to pure function calls. (Constants considered a special case of pure functions.)

# A done-queue worker picks up a result from the Done queue.
#     The worker examines the waiting queue/map for calculations waiting for this result.
#     For each found:
#         If this is all the waitee is waiting for:
#             The worker adds an item to the Ready queue to evaluate the function with evaluated arguments from the Results DB;
#             and removes the item from the Waiting queue.
#         If this is one of muliple results the waitee is waiting for:
#             The worker adds a new item to the Waiting queue with one less dependency;
#             and removes the original item from the Waiting queue.

# Results DB is redis, memcached type. Possbly with multiple levels and caches near the workers which would make
# affinity matter.


#-------------------------------------------------------------------------------

def timeStampStr():
    '''Standardizes time stamp format'''
    return '{:.10f} {:s}'.format(time.monotonic(), time.strftime("%Y%m%d%H%S", time.gmtime()))

def reqIdFromCode(code):
    '''Standardizes code -> request ID generation'''
    return hash(code)

NonExistingResult = namedtuple('NonExistingResult', ['reqID'] )

#WaitingRequest = namedtuple('WaitingRequest', ['deps', 'reqID', 'code'])
class WaitingRequest( object ):
    def __init__(self, deps, reqID, code):
        self.deps = deps
        self.reqID = reqID
        self.code = code

Comms = namedtuple( 'Comms', [  'resultsDB',        # Db where results are stored
                                'toBeCalculatedQ',  # Queue for calculations ready to run
                                'toBeAnnouncedQ',   # Queue for calculations finished which need announcing to other workers
                                'toBeReportedQ',    # Queue for calculations finished which need to be reported to user
                                'toBeRevivedQ',     # Queue for calculations waiting for inputs to be ready
                                'toBeLoggedQ',      # Queue for messages to be logged
                                'resultSendrs',     # Pipe ends where completed request IDs are sent
                             ] )

#-------------------------------------------------------------------------------
# Mini language we interpret

class Language( object ):
    '''We're onlly going to have built in functions or forms.'''

    class SpecialForms( object ):
        @staticmethod
        def quote( arg ):
            return arg

    class Functions( object ):
        @staticmethod
        def sum( *args ):
            return functools.reduce( operator.add, args, 0. )

        @staticmethod
        def diff( *args ):
            return functools.reduce( operator.sub, args, 0. )

        @staticmethod
        def prod( *args ):
            return functools.reduce( operator.mul, args, 1. )

        @staticmethod
        def frac( *args ):
            return functools.reduce( operator.truediv, args, 1. )

        @staticmethod
        def evalsToSum():
            return 'sum'

    @staticmethod
    def eval( code ):
        '''Not lisp, just lispish.'''
        if not code:
            return code
        elif isinstance( code[0], tuple ):
            return Language.eval( ( Language.eval( code[0] ), ) + code[1:] )
        elif hasattr( Language.SpecialForms, code[0] ):
            return getattr( Language.SpecialForms, code[0] )( *code[1:] )
        else:
            code = [ Language.eval( x ) if isinstance( x, tuple ) else x for x in code ]
            return getattr( Language.Functions, code[0] )( *code[1:] )



#-------------------------------------------------------------------------------
# Workers

def calculationWorkerMethod( workerID, comms):
    # NOTE: A request shouldn't be revived unless _all_ inputs are ready. So we won't attempt to handle partial ready results.
    while True:
        reqID, code = comms.toBeCalculatedQ.get()
        if reqID not in comms.resultsDB:
            # If it was in the db there'd be nothing for us to do
            if not code:
                res = Language.eval(code)
                comms.resultsDB[reqID] = res
                comms.toBeReportedQ.put( reqID )
                comms.toBeAnnouncedQ.put( reqID )
                #comms.toBeLoggedQ.put('[{:s}] {:s} completed {:x} -> {:s}'.format(timeStampStr(), str(workerID), reqID, str(res)))
            elif any( isinstance(c, tuple) for c in code ):
                # We have sub-code to evaluate
                newCode = []
                for c in code:
                    if isinstance(c, tuple):
                        # Nested function call
                        # FIXME: check if results is already in db
                        childReqID = reqIdFromCode(c)
                        if childReqID not in comms.resultsDB:
                            comms.toBeCalculatedQ.put((childReqID, c))
                            newCode.append(NonExistingResult(childReqID))
                        else:
                            newCode.append(comms.resultsDB[childReqID])
                    else:
                        # Plain old data
                        newCode.append(c)
                # Preserve already existing ID. Which is now no longer aligned with hash.
                comms.toBeRevivedQ.put((reqID, tuple(newCode)))
                #comms.toBeLoggedQ.put('[{:s}] {:s} sending {:x} to toBeRevivedQ.'.format(timeStampStr(), str(workerID), reqID))
            else:
                # No sub-code, just evaluate.
                res = Language.eval([ resultsDB[c.reqID] if isinstance(c, NonExistingResult) else c for c in code ])
                comms.resultsDB[ reqID ] = res
                comms.toBeReportedQ.put( reqID )
                comms.toBeAnnouncedQ.put( reqID )
                #comms.toBeLoggedQ.put('[{:s}] {:s} completed {:x} -> {:s}'.format(timeStampStr(), str(workerID), reqID, str(res)))


def reportWorkerMethod( workerID, comms ):
    '''Report result to end user'''
    while True:
        reqID = comms.toBeReportedQ.get()
        # Imagine this getting back to the user somehow
        comms.toBeLoggedQ.put("[{:s}] {:s} DONE {:x}".format(timeStampStr(), str(workerID), reqID))

def announcementWorkerMethod(workerID, comms):
    '''Announce results to other workers'''
    # Exclusive writer to the result announcing pipes. Exactly one of these. Must be quick.
    # Questionable method
    while True:
        reqID = comms.toBeAnnouncedQ.get()
        for pe in comms.resultSendrs:
            pe.send(reqID) # Hope this doesn't block (buffer full)
            #comms.toBeLoggedQ.put("[{:s}] {:s} Announcing {:x}".format(timeStampStr(), str(workerID), reqID))
            # A subscription model where only some workers get some messages might be better.

def revivalWorkerMethod(workerID, comms, pipeEnd):
    '''Restart calculations which were on hold waiting for inputs'''
    # Questionable method
    tasks = []

    def updateTask(task, finishedReqID):
        '''Update task based on knowledge of new finishedReqID'''
        if finishedReqID not in task.deps:
            return task
        else:
            task.code = [comms.resultsDB[c.reqID] if isinstance(c, NonExistingResult) and c.reqID == finishedReqID else c for c in task.code]
            task.deps.remove(finishedReqID)
            if task.deps:
                # There are still unmet deps
                return task
            else:
                # This guy is ready
                # Do something
                comms.toBeCalculatedQ.put((task.reqID, task.code))
                return None

    while True:
        if pipeEnd.poll():
            # Listen for announcements that other workers have completed calcualtions and see if any of our tasks depend on them.
            finishedReqID = pipeEnd.recv()
            #comms.toBeLoggedQ.put("[{:s}] {:s} Noticed {:x}".format(timeStampStr(), str(workerID), finishedReqID))
            tasks = [updateTask(t, finishedReqID) for t in tasks if t is not None]

        if not comms.toBeRevivedQ.empty():
            # Docs say empty() is unreliable so we still could get an exception
            try:
                # Pick up new tasks
                reqID, code = comms.toBeRevivedQ.get_nowait()
            except queue.Empty:
                pass # Nothing in queue, keep polling
            else:
                # We got a new one
                # Some results could have come in since this was added to the queue, so check now
                code = [comms.resultsDB[c.reqID] if isinstance(c, NonExistingResult) and c.reqID == finishedReqID else c for c in code]
                deps = set((c.reqID for c in code if isinstance(c, NonExistingResult)))
                newTask = WaitingRequest(deps, reqID, code)
                tasks.append(newTask)
                #comms.toBeLoggedQ.put("[{:s}] {:s} Took on {:x} with deps {:s}".format(timeStampStr(), str(workerID), reqID, str(deps)))

def logWorkerMethod( workerID, comms ):
    while True:
        msg =  comms.toBeLoggedQ.get()
        print(msg)






#-------------------------------------------------------------------------------
# Example

if __name__ == '__main__':
    nCalculationWorkers = 4
    nReportWorkers = 2
    nRevivalWorkers = 2
    nLogWorkers = 1 # Keep at 1 or print statements will get mixed together
    with mp.Manager() as resultsDBMgr:
        resultsDB = resultsDBMgr.dict()
        toBeCalculatedQ = mp.Queue()
        toBeAnnouncedQ = mp.Queue()
        toBeReportedQ = mp.Queue()
        toBeRevivedQ = mp.Queue()
        toBeLoggedQ = mp.Queue()
        resultRecvrs, resultSendrs = zip(*(mp.Pipe() for _ in range(nRevivalWorkers)))

        comms = Comms(resultsDB,
                      toBeCalculatedQ,
                      toBeAnnouncedQ,
                      toBeReportedQ,
                      toBeRevivedQ,
                      toBeLoggedQ,
                      resultSendrs)

        print(comms)

        calculationWorkers = [mp.Process(target=calculationWorkerMethod, args=((i, 'C'), comms)) for i in range(nCalculationWorkers)]
        for w in calculationWorkers: w.start()

        announcementWorker = mp.Process(target=announcementWorkerMethod, args=((0,'A'), comms))
        announcementWorker.start()

        reportWorkers = [mp.Process(target=reportWorkerMethod, args=((i, 'R'), comms)) for i in range(nReportWorkers)]
        for w in reportWorkers: w.start()

        revivalWorkers = [mp.Process(target=revivalWorkerMethod, args=((i, 'V'), comms, pipeEnd)) for i, pipeEnd in enumerate(resultRecvrs)]
        for w in revivalWorkers: w.start()

        logWorkers = [mp.Process(target=logWorkerMethod, args=((i, 'L'), comms)) for i in range(nRevivalWorkers)]
        for w in logWorkers: w.start()

        for code in [ ( 'sum', 1, ( 'prod', 2, 3 ) ),
                      ( 'frac', 1, 2, 3, 4 ),
                      ( 'frac', ( 'sum', 1., 2. ), ( 'prod', 3., 4. ) ),
                      ( 'frac', ( ( 'evalsToSum', ), 1., 2. ), ( 'prod', 3., 4. ) ),
                    ]:
            reqID = reqIdFromCode(code)
            toBeLoggedQ.put("User requesting {:x}".format(reqID))
            toBeCalculatedQ.put((reqID, code))


        for w in calculationWorkers + [ announcementWorker ] + reportWorkers + revivalWorkers + logWorkers:
            w.join()