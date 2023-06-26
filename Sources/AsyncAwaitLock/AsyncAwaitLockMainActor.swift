import Foundation


// WARNING: AsyncAwaitLockMainActor doesn't have unit tests yet and is suposed to be referenced from a @MainActor class and used inside an @MainActor environment. Perhaps Swift 5.9 custom executors will also help put multiple actors on the same thread and relax actor isolation for a group of actors? One can dream.
//
// WARNING: Only AsyncAwaitLock has unit tests.
//
// Code on @MainActor runs on the main thread
// for which the main benefit is no additional penalty for context switching between threads
// if also only used by code on the main actor.

@MainActor
public class AsyncAwaitLockMainActor: CustomStringConvertible {
    public typealias LockID = UInt64
    
    public enum LockError: Error {
        case disposed(name: String)
        case notAcquired(lock: AsyncAwaitLockMainActor?)
        case acquiredElsewhere(lock: AsyncAwaitLockMainActor, (file: String, line: Int)? = nil)
        case replaced(lock: AsyncAwaitLockMainActor, (file: String, line: Int)? = nil)
    }
    
    public nonisolated let name: String
    public nonisolated var description: String {
        name
    }
    
    public var isAcquired: Bool {
        acquiredLockID != nil
    }
    
    public private(set) var acquiredLockID: LockID? = nil
    
    private var lockID: LockID = 0
    
    private var preventNewAcquires: Bool = false
    private var continuationsFIFO: [CheckedContinuation<Void, any Error>] = []
    private var continuationLockIDsFIFO: [LockID] = []
    private var prematureReleaseLockIDs: Set<LockID> = []
    private var debugLockIDToFileAndLine: Dictionary<LockID, (file: String, line: Int)> = [:]
    
    // From Apple’s docs: The checked continuation offers detection of mis-use,
    // and dropping the last reference to it,
    // without having resumed it will trigger a warning.
    // Resuming a continuation twice is also diagnosed and will cause a crash.
    deinit {
        if continuationsFIFO.count > 0 {
            print("WARNING: AsyncAwaitSemaphore named \(name) unlocked inside deinit.")
        }
        
        let continuations = continuationsFIFO
        continuationsFIFO.removeAll()
        continuationLockIDsFIFO.removeAll()
        debugLockIDToFileAndLine.removeAll()
        
        for continuation in continuations {
            continuation.resume(throwing: LockError.disposed(name: name))
        }
        
        // .release() for the acquired lock will be reached and will not throw.
        if acquiredLockID != nil {
            prematureReleaseLockIDs.insert(acquiredLockID!)
            acquiredLockID = nil
        }
    }
    
    
    // Same as deinit, except it can be called purposefully.
    private var disposed = false
    public func dispose() {
        if continuationsFIFO.count > 0 {
            print("WARNING: AsyncAwaitSemaphore named \(name) unlocked inside dispose().")
        }
        
        failNewAcquires()
        failAllInner(error: LockError.disposed(name: name))
        
        waitAllWaitLock?.failAllInner(error: LockError.disposed(name: name))
        waitAllWaitLock?.dispose()
        waitAllWaitLock = nil

        disposed = true
    }
    
    
    public init(name: String) {
        self.name = name
    }
    
    
    public func acquireNonWaiting(file: String? = nil, line: Int? = nil) async throws -> LockID {
        if isAcquired {
            throw LockError.acquiredElsewhere(lock: self, debugLockIDToFileAndLine[acquiredLockID!])
        }
        
        return try await acquire(file: file, line: line)
    }
    
    
    public func acquire(
        timeout: TimeInterval? = nil,
        replaceWaiting: Bool = false,
        file: String? = nil,
        line: Int? = nil
    ) async throws -> LockID {
        if disposed {
            fatalError("Attempted to lock disposed() lock named \(name)")
        }
        
        if preventNewAcquires {
            throw LockError.notAcquired(lock: self)
        }
        
        lockID += 1
        let lockID = lockID
        
        if isAcquired == false {
            acquiredLockID = lockID
        }
        else {
            if replaceWaiting {
                let continuations = continuationsFIFO
                continuationsFIFO.removeAll(keepingCapacity: true)
                continuationLockIDsFIFO.removeAll(keepingCapacity: true)
                
                for continuation in continuations {
                    continuation.resume(throwing: LockError.replaced(lock: self, debugLockIDToFileAndLine[lockID]))
                }
            }
            
            try await withCheckedThrowingContinuation { [self] continuation in
                self.continuationLockIDsFIFO.append(lockID)
                self.continuationsFIFO.append(continuation)
                
                if timeout != nil {
                    Task { [self] in
                        try! await Task.sleep(nanoseconds: UInt64(1_000_000_000 * timeout!))
                        
                        let index = self.continuationLockIDsFIFO.firstIndex(of: lockID)
                        if index != nil {
                            self.continuationLockIDsFIFO.remove(at: index!)
                            self.continuationsFIFO.remove(at: index!)
                            continuation.resume(throwing: LockError.notAcquired(lock: self))
                        }
                    }
                }
            }
            
            acquiredLockID = lockID
        }
        
        if file != nil || line != nil {
            debugLockIDToFileAndLine[acquiredLockID!] = (file: file ?? "", line: line ?? -1)
        }
        
        return acquiredLockID!
    }
    
    
    public func release(acquiredLockID: LockID, ignoreRepeatRelease: Bool = false) throws {
        if prematureReleaseLockIDs.contains(acquiredLockID) {
            prematureReleaseLockIDs.remove(acquiredLockID)
            
            return
        }
        
        
        var isAlreadyReleased = false
        if isAcquired == false {
            if ignoreRepeatRelease == false {
                throw LockError.notAcquired(lock: self)
            }
            else {
                isAlreadyReleased = true
            }
        }
        
        // if acquiredLockID != nil {
        if self.acquiredLockID != acquiredLockID {
            if ignoreRepeatRelease == false {
                throw LockError.acquiredElsewhere(lock: self, debugLockIDToFileAndLine[self.acquiredLockID!])
            }
            else {
                isAlreadyReleased = true
            }
        }
        // }
        
        if isAlreadyReleased {
            return
        }
        
        
        if self.continuationsFIFO.count > 0 {
            let continuation = self.continuationsFIFO.removeFirst()
            let lockIDRemoved = self.continuationLockIDsFIFO.removeFirst()
            debugLockIDToFileAndLine.removeValue(forKey: lockIDRemoved)
            
            continuation.resume()
        }
        else {
            self.acquiredLockID = nil
            debugLockIDToFileAndLine.removeAll(keepingCapacity: true)
            
            if waitAllLockID != nil {
                try! waitAllWaitLock!.release(acquiredLockID: waitAllLockID!, ignoreRepeatRelease: true)
            }
        }
    }
    
    
    public func whereAcquired() throws -> (file: String, line: Int)? {
        if isAcquired == false {
            throw LockError.notAcquired(lock: self)
        }
        
        return debugLockIDToFileAndLine[self.acquiredLockID!]
    }
    
    
    public func failNewAcquires() {
        preventNewAcquires = true
    }
    
    public func allowNewAcquires() {
        assert(disposed == false)
        
        preventNewAcquires = false
    }
    
    
    private var waitAllWaitLock: AsyncAwaitLockMainActor? = nil
    private var waitAllLockID: LockID? = nil
    public func waitAll() async throws {
        if disposed {
            fatalError("Attempted to waitAll() on disposed() lock named \(name)")
        }
        
        let waitAllLockName = "__waitAllWaitLock__"
        if name == waitAllLockName {
            fatalError("\(waitAllLockName) is a reserved name.")
        }
        
        if isAcquired == false {
            assert(continuationsFIFO.isEmpty)
            return
        }
        
        if waitAllWaitLock == nil {
            waitAllWaitLock = AsyncAwaitLockMainActor(name: waitAllLockName)
        }
        let waitAllWaitLock = waitAllWaitLock!
        waitAllLockID = try await waitAllWaitLock.acquireNonWaiting()
        
        let waitAllLockIDWait = try! await waitAllWaitLock.acquire()
        waitAllLockID = nil
        
        try! waitAllWaitLock.release(acquiredLockID: waitAllLockIDWait)
    }
    
    
    internal func failAllInner(error: LockError) {
        let continuations = continuationsFIFO
        continuationsFIFO.removeAll(keepingCapacity: true)
        continuationLockIDsFIFO.removeAll(keepingCapacity: true)
        debugLockIDToFileAndLine.removeAll(keepingCapacity: true)
        
        for continuation in continuations {
            continuation.resume(throwing: error)
        }
        
        // .release() for the acquired lock will be reached and will not throw.
        if isAcquired {
            prematureReleaseLockIDs.insert(acquiredLockID!)
            acquiredLockID = nil
        }
    }
    
    
    public func failAll() throws {
        failAllInner(error: LockError.notAcquired(lock: self))
        
        if disposed == true {
            return
        }
        
        if waitAllLockID != nil {
            let waitAllLockIDCopy = waitAllLockID!
            waitAllLockID = nil
            try! waitAllWaitLock!.release(acquiredLockID: waitAllLockIDCopy, ignoreRepeatRelease: true)
        }
    }
    
    
    public func checkReleased() throws {
        if isAcquired {
            throw LockError.acquiredElsewhere(lock: self, debugLockIDToFileAndLine[self.acquiredLockID!])
        }
        
        assert(continuationsFIFO.isEmpty)
        assert(continuationLockIDsFIFO.isEmpty)
        assert(debugLockIDToFileAndLine.isEmpty)
    }
}
