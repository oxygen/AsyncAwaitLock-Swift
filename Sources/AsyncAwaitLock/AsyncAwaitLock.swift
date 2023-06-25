import Foundation
import Dispatch


public actor AsyncAwaitLock {
    public typealias LockID = UInt64
    
    public enum LockError: Error {
        case notAcquired
        case acquiredElsewhere((file: String, line: Int)? = nil)
        case replaced((file: String, line: Int)? = nil)
    }
    
    public var name: String
    
    public var isAcquired: Bool {
        acquiredLockID != nil
    }
    
    public private(set) var acquiredLockID: LockID? = nil
    
    private var lockID: LockID = 0
     
    private var continuationsFIFO: [CheckedContinuation<Void, Never>] = []
    private var continuationLockIDs: [LockID] = []
    private var replacedContinuationsLockIDs: Set<LockID> = []
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
            throw LockError.acquiredElsewhere(debugLockIDToFileAndLine[acquiredLockID!])
        }
        
        return try await acquire(file: file, line: line)
    }
    
    
    public func acquire(replaceWaiting: Bool = false, file: String? = nil, line: Int? = nil) async throws -> LockID {
        if disposed {
            fatalError("Attempted to lock disposed() lock named \(name)")
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
                    let continuation = self.continuationsFIFO.first!
                    
                    self.continuationsFIFO.removeFirst(1)
                    self.continuationLockIDs.removeFirst(1)
                    
                    continuation.resume()
                }
                
                throw LockError.replaced(debugLockIDToFileAndLine[self.lastReplacingContinuationLockID!])
            }
            
            acquiredLockID = lockID
        }
        
        if file != nil || line != nil {
            debugLockIDToFileAndLine[acquiredLockID!] = (file: file ?? "", line: line ?? -1)
        }
        
        return acquiredLockID!
    }
    
    
    public func release(acquiredLockID: LockID?) throws {
        if disposed {
            fatalError("Attempted to release disposed() lock named \(name)")
        }
        
        if isAcquired == false {
            throw LockError.notAcquired
        }
        
        if acquiredLockID != nil {
            if self.acquiredLockID != acquiredLockID {
                throw LockError.acquiredElsewhere(debugLockIDToFileAndLine[self.acquiredLockID!])
            }
        }
        
        if self.continuationsFIFO.count > 0 {
            let continuation = self.continuationsFIFO.first!
            
            self.continuationsFIFO.removeFirst(1)
            self.continuationLockIDs.removeFirst(1)
            
            continuation.resume()
        }
        else {
            self.acquiredLockID = nil
        }
    }
    
    
    public func whereAcquired() throws -> (file: String, line: Int)? {
        if isAcquired == false {
            throw LockError.notAcquired
        }
        
        return debugLockIDToFileAndLine[self.acquiredLockID!]
    }
    
    
    public func checkReleased() throws {
        if isAcquired {
            throw LockError.acquiredElsewhere(debugLockIDToFileAndLine[self.acquiredLockID!])
        }
    }
}


@MainActor
public class AsyncAwaitLockMainActor {
    public typealias LockID = UInt64
    
    public enum LockError: Error {
        case notAcquired
        case acquiredElsewhere((file: String, line: Int)? = nil)
        case replaced((file: String, line: Int)? = nil)
    }
    
    public var name: String
    
    public var isAcquired: Bool {
        acquiredLockID != nil
    }
    
    public private(set) var acquiredLockID: LockID? = nil
    
    private var lockID: LockID = 0
     
    private var continuationsFIFO: [CheckedContinuation<Void, Never>] = []
    private var continuationLockIDs: [LockID] = []
    private var replacedContinuationsLockIDs: Set<LockID> = []
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
            throw LockError.acquiredElsewhere(debugLockIDToFileAndLine[acquiredLockID!])
        }
        
        return try await acquire(file: file, line: line)
    }
    
    
    public func acquire(replaceWaiting: Bool = false, file: String? = nil, line: Int? = nil) async throws -> LockID {
        if disposed {
            fatalError("Attempted to lock disposed() lock named \(name)")
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
                    let continuation = self.continuationsFIFO.first!
                    
                    self.continuationsFIFO.removeFirst(1)
                    self.continuationLockIDs.removeFirst(1)
                    
                    continuation.resume()
                }
                
                throw LockError.replaced(debugLockIDToFileAndLine[self.lastReplacingContinuationLockID!])
            }
            
            acquiredLockID = lockID
        }
        
        if file != nil || line != nil {
            debugLockIDToFileAndLine[acquiredLockID!] = (file: file ?? "", line: line ?? -1)
        }
        
        return acquiredLockID!
    }
    
    
    public func release(acquiredLockID: LockID?) throws {
        if disposed {
            fatalError("Attempted to release disposed() lock named \(name)")
        }
        
        if isAcquired == false {
            throw LockError.notAcquired
        }
        
        if acquiredLockID != nil {
            if self.acquiredLockID != acquiredLockID {
                throw LockError.acquiredElsewhere(debugLockIDToFileAndLine[self.acquiredLockID!])
            }
        }
        
        if self.continuationsFIFO.count > 0 {
            let continuation = self.continuationsFIFO.first!
            
            self.continuationsFIFO.removeFirst(1)
            self.continuationLockIDs.removeFirst(1)
            
            continuation.resume()
        }
        else {
            self.acquiredLockID = nil
        }
    }
    
    
    public func whereAcquired() throws -> (file: String, line: Int)? {
        if isAcquired == false {
            throw LockError.notAcquired
        }
        
        return debugLockIDToFileAndLine[self.acquiredLockID!]
    }
    
    
    public func checkReleased() throws {
        if isAcquired {
            throw LockError.acquiredElsewhere(debugLockIDToFileAndLine[self.acquiredLockID!])
        }
    }
}
