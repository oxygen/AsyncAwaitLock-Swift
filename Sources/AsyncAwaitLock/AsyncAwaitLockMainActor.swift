import Foundation


@MainActor
public class AsyncAwaitLockMainActor: CustomStringConvertible {
    public typealias LockID = UInt64
    
    public enum LockError: Error {
        case disposed(name: String)
        case notAcquired(lock: AsyncAwaitLockMainActor)
        case prevented(lock: AsyncAwaitLockMainActor)
        case expresslyFailed(lock: AsyncAwaitLockMainActor, methodName: String)
        case timedOutWaiting(lock: AsyncAwaitLockMainActor, timeout: TimeInterval)
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
    private var continuationsAndLockIDsFIFO: [(
        continuation: CheckedContinuation<Void, any Error>,
        lockID: LockID
    )] = []
    private var prematureReleaseLockIDs: Set<LockID> = []
    private var debugLockIDToFileAndLine: Dictionary<LockID, (file: String, line: Int)> = [:]
    
    // From Apple’s docs: The checked continuation offers detection of mis-use,
    // and dropping the last reference to it,
    // without having resumed it will trigger a warning.
    // Resuming a continuation twice is also diagnosed and will cause a crash.
    deinit {
        if continuationsAndLockIDsFIFO.count > 0 {
            print("WARNING: AsyncAwaitSemaphore named \(name) unlocked inside deinit.")
        }
        
        let continuations = continuationsAndLockIDsFIFO
        continuationsAndLockIDsFIFO.removeAll()
        debugLockIDToFileAndLine.removeAll()
        
        for (continuation, _) in continuations {
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
        if continuationsAndLockIDsFIFO.count > 0 {
            print("WARNING: AsyncAwaitSemaphore named \(name) unlocked inside dispose().")
        }
        
        failNewAcquires()
        failAllInner(error: LockError.disposed(name: name), onlyWaiting: false)
        
        waitAllWaitLock?.failAllInner(error: LockError.disposed(name: name), onlyWaiting: false)
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
            throw LockError.prevented(lock: self)
        }
        
        lockID += 1
        let lockID = lockID
        
        if isAcquired == false {
            acquiredLockID = lockID
        }
        else {
            if replaceWaiting {
                let continuations = continuationsAndLockIDsFIFO
                continuationsAndLockIDsFIFO.removeAll(keepingCapacity: true)
                
                for (continuation, _) in continuations {
                    continuation.resume(throwing: LockError.replaced(lock: self, debugLockIDToFileAndLine[lockID]))
                }
            }
            
            try await withCheckedThrowingContinuation { [self] continuation in
                continuationsAndLockIDsFIFO.append((
                    continuation: continuation,
                    lockID: lockID
                ))
                
                if timeout != nil {
                    Task { [self] in
                        try! await Task.sleep(nanoseconds: UInt64(1_000_000_000 * timeout!))
                        
                        let index = continuationsAndLockIDsFIFO.firstIndex(where: { $0.lockID == lockID })
                        if index != nil {
                            continuationsAndLockIDsFIFO.remove(at: index!)
                            continuation.resume(throwing: LockError.timedOutWaiting(lock: self, timeout: timeout!))
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
        
        
        if continuationsAndLockIDsFIFO.count > 0 {
            let (continuation, lockIDRemoved) = continuationsAndLockIDsFIFO.removeFirst()
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
            assert(continuationsAndLockIDsFIFO.isEmpty)
            return
        }
        
        if waitAllWaitLock == nil {
            waitAllWaitLock = AsyncAwaitLockMainActor(name: waitAllLockName)
        }
        let waitAllWaitLock = waitAllWaitLock!
        waitAllLockID = try await waitAllWaitLock.acquireNonWaiting()
        
        let waitAllLockIDWait: LockID
        do {
            waitAllLockIDWait = try await waitAllWaitLock.acquire()
        }
        catch {
            switch error as! LockError {
            case .disposed: return
            case .expresslyFailed: return
            case .acquiredElsewhere: fatalError(error.localizedDescription)
            case .prevented: fatalError(error.localizedDescription)
            case .notAcquired: fatalError(error.localizedDescription)
            case .timedOutWaiting: fatalError(error.localizedDescription)
            case .replaced: fatalError(error.localizedDescription)
            }
        }
        
        waitAllLockID = nil
        try! waitAllWaitLock.release(acquiredLockID: waitAllLockIDWait)
    }
    
    
    internal func failAllInner(error: LockError, onlyWaiting: Bool) {
        let continuations = continuationsAndLockIDsFIFO
        continuationsAndLockIDsFIFO.removeAll(keepingCapacity: true)
        debugLockIDToFileAndLine.removeAll(keepingCapacity: true)
        
        for (continuation, _) in continuations {
            continuation.resume(throwing: error)
        }
        
        // .release() for the acquired lock will be reached and will not throw.
        if isAcquired && onlyWaiting == false {
            prematureReleaseLockIDs.insert(acquiredLockID!)
            acquiredLockID = nil
        }
    }
    
    
    public func failAll(onlyWaiting: Bool = false) throws {
        failAllInner(error: LockError.expresslyFailed(lock: self, methodName: #function), onlyWaiting: onlyWaiting)
        
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
        
        assert(continuationsAndLockIDsFIFO.isEmpty)
        assert(debugLockIDToFileAndLine.isEmpty)
    }
}
