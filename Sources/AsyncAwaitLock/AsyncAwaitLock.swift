import Foundation
import Dispatch


public actor AsyncAwaitLock: CustomStringConvertible {
    public typealias LockID = UInt64
    
    public enum LockError: Error {
        case notAcquired(lock: AsyncAwaitLock)
        case acquiredElsewhere(lock: AsyncAwaitLock, (file: String, line: Int)? = nil)
        case replaced(lock: AsyncAwaitLock, (file: String, line: Int)? = nil)
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
    private var continuationsFIFO: [CheckedContinuation<Void, Never>] = []
    private var continuationLockIDs: [LockID] = []
    private var replacedContinuationsLockIDs: Set<LockID> = []
    private var failedContinuationsLockIDs: Set<LockID> = []
    private var prematureReleaseLockIDs: Set<LockID> = []
    private var lastReplacingContinuationLockID: LockID? = nil
    private var debugLockIDToFileAndLine: Dictionary<LockID, (file: String, line: Int)> = [:]
    
    // From Apple’s docs: The checked continuation offers detection of mis-use,
    // and dropping the last reference to it,
    // without having resumed it will trigger a warning.
    // Resuming a continuation twice is also diagnosed and will cause a crash.
    deinit {
        if continuationsFIFO.count > 0 {
            print("WARNING: AsyncAwaitSemaphore named \(name) unlocked inside deinit.")
            
            let continuations = continuationsFIFO
            
            self.continuationsFIFO.removeAll() // Unnecessary?
            
            for continuation in continuations {
                continuation.resume()
            }
        }
    }
    
    
    // Same as deinit, except it can be called purposefully.
    private var disposed = false
    public func dispose() {
        if continuationsFIFO.count > 0 {
            print("WARNING: AsyncAwaitSemaphore named \(name) unlocked inside dispose().")
            
            let continuations = continuationsFIFO
            
            self.continuationsFIFO.removeAll()
            
            for continuation in continuations {
                continuation.resume()
            }
        }
        
        acquiredLockID = nil
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
    
    
    public func acquire(replaceWaiting: Bool = false, file: String? = nil, line: Int? = nil) async throws -> LockID {
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
                let continuations = self.continuationsFIFO
                self.continuationsFIFO.removeAll(keepingCapacity: true)
                
                replacedContinuationsLockIDs.reserveCapacity((replacedContinuationsLockIDs.count + continuationLockIDs.count) * 2)
                for replacedLockID in continuationLockIDs {
                    replacedContinuationsLockIDs.insert(replacedLockID)
                }
                continuationLockIDs.removeAll(keepingCapacity: true)
                
                lastReplacingContinuationLockID = lockID
                
                // All of these will now throw .replaced
                for continuation in continuations {
                    continuation.resume()
                }
            }
            
            await withCheckedContinuation { continuation in
                self.continuationLockIDs.append(lockID)
                self.continuationsFIFO.append(continuation)
            }
            
            if replacedContinuationsLockIDs.contains(lockID) {
                replacedContinuationsLockIDs.remove(lockID)
                
                // Like .release()
                if isAcquired == false && self.continuationsFIFO.count > 0 {
                    let continuation = self.continuationsFIFO.removeFirst()
                    let lockIDRemoved = self.continuationLockIDs.removeFirst()
                    debugLockIDToFileAndLine.removeValue(forKey: lockIDRemoved)
                    
                    continuation.resume()
                }
                
                throw LockError.replaced(lock: self, debugLockIDToFileAndLine[self.lastReplacingContinuationLockID!])
            }
            
            else if failedContinuationsLockIDs.contains(lockID) {
                failedContinuationsLockIDs.remove(lockID)
                
                if failAllLockID != nil && failedContinuationsLockIDs.isEmpty {
                    try! await failAllWaitLock!.release(acquiredLockID: failAllLockID!, ignoreRepeatRelease: true)
                }
                
                // Like .release()
                if isAcquired == false && self.continuationsFIFO.count > 0 {
                    let continuation = self.continuationsFIFO.removeFirst()
                    let lockIDRemoved = self.continuationLockIDs.removeFirst()
                    debugLockIDToFileAndLine.removeValue(forKey: lockIDRemoved)
                    
                    continuation.resume()
                }
                
                throw LockError.notAcquired(lock: self)
            }
            
            acquiredLockID = lockID
        }
        
        if file != nil || line != nil {
            debugLockIDToFileAndLine[acquiredLockID!] = (file: file ?? "", line: line ?? -1)
        }
        
        return acquiredLockID!
    }
    
    
    public func release(acquiredLockID: LockID, ignoreRepeatRelease: Bool = false) async throws {
        if disposed {
            fatalError("Attempted to release disposed() lock named \(name)")
        }
        
        if prematureReleaseLockIDs.contains(acquiredLockID) {
            prematureReleaseLockIDs.remove(acquiredLockID)
            
            if failAllLockID != nil && failedContinuationsLockIDs.isEmpty {
                try! await failAllWaitLock!.release(acquiredLockID: failAllLockID!, ignoreRepeatRelease: true)
            }
            
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
            let lockIDRemoved = self.continuationLockIDs.removeFirst()
            debugLockIDToFileAndLine.removeValue(forKey: lockIDRemoved)
            
            continuation.resume()
        }
        else {
            self.acquiredLockID = nil
            debugLockIDToFileAndLine.removeAll(keepingCapacity: true)
            
            if waitAllLockID != nil {
                try! await waitAllWaitLock!.release(acquiredLockID: waitAllLockID!, ignoreRepeatRelease: true)
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
        preventNewAcquires = false
    }
    
    
    private var waitAllWaitLock: AsyncAwaitLock? = nil
    private var waitAllLockID: LockID? = nil
    public func waitAll() async throws {
        let waitAllLockName = "__waitAllWaitLock__"
        if name == waitAllLockName {
            fatalError("\(waitAllLockName) is a reserved name.")
        }
        
        if isAcquired == false {
            assert(continuationsFIFO.isEmpty)
            return
        }
        
        if waitAllWaitLock == nil {
            waitAllWaitLock = Self(name: waitAllLockName)
        }
        let waitAllWaitLock = waitAllWaitLock!
        waitAllLockID = try await waitAllWaitLock.acquireNonWaiting()
        
        let waitAllLockIDWait = try! await waitAllWaitLock.acquire()
        waitAllLockID = nil
        
        try! await waitAllWaitLock.release(acquiredLockID: waitAllLockIDWait)
    }
    
    
    private var failAllWaitLock: AsyncAwaitLock? = nil
    private var failAllLockID: LockID? = nil
    public func failAll() async throws {
        let failAllLockName = "__failAllWaitLock__"
        if name == failAllLockName {
            fatalError("\(failAllLockName) is a reserved name.")
        }
        
        if failAllWaitLock == nil {
            failAllWaitLock = Self(name: failAllLockName)
        }
        let failAllWaitLock = failAllWaitLock!
        failAllLockID = try await failAllWaitLock.acquireNonWaiting()
        
        let continuations = self.continuationsFIFO
        self.continuationsFIFO.removeAll(keepingCapacity: true)
        
        failedContinuationsLockIDs.reserveCapacity((failedContinuationsLockIDs.count + continuationLockIDs.count) * 2)
        for replacedLockID in continuationLockIDs {
            failedContinuationsLockIDs.insert(replacedLockID)
        }
        continuationLockIDs.removeAll(keepingCapacity: true)
        
        // All of these will now throw .notAcquired
        for continuation in continuations {
            continuation.resume()
        }
        
        // .release() for the acquired lock will be reached and will not throw.
        if isAcquired {
            prematureReleaseLockIDs.insert(acquiredLockID!)
            acquiredLockID = nil
        }

        
        if failedContinuationsLockIDs.count > 0 {
            let failAllLockIDWait = try! await failAllWaitLock.acquire()
            failAllLockID = nil
            
            // This shouldn't still hold stuff but it does (maybe from the acquired lock
            // which we don't await for since it could take forever or deadlock).
            debugLockIDToFileAndLine.removeAll(keepingCapacity: true)
            
            try! await failAllWaitLock.release(acquiredLockID: failAllLockIDWait)
        }
        else {
            let failAllLockIDCopy = failAllLockID!
            failAllLockID = nil
            
            // This shouldn't still hold stuff but it does (maybe from the acquired lock
            // which we don't await for since it could take forever or deadlock).
            debugLockIDToFileAndLine.removeAll(keepingCapacity: true)
            
            try! await failAllWaitLock.release(acquiredLockID: failAllLockIDCopy)
        }
        
        if waitAllLockID != nil {
            try! await waitAllWaitLock!.release(acquiredLockID: waitAllLockID!, ignoreRepeatRelease: true)
        }
    }
    
    
    public func checkReleased() throws {
        if isAcquired {
            throw LockError.acquiredElsewhere(lock: self, debugLockIDToFileAndLine[self.acquiredLockID!])
        }
        
        assert(continuationsFIFO.isEmpty)
        assert(continuationLockIDs.isEmpty)
        assert(replacedContinuationsLockIDs.isEmpty)
        assert(failedContinuationsLockIDs.isEmpty)
        assert(debugLockIDToFileAndLine.isEmpty)
    }
}
