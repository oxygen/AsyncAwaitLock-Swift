import Foundation


@MainActor
public class AsyncAwaitLockMainActor: CustomStringConvertible {
    public typealias LockID = UInt64
    
    public struct SourceLocation {
        let file: String
        let line: Int
        let time: Date
        
        public init(file: String, line: Int, time: Date = Date()) {
            self.file = file
            self.line = line
            self.time = time
        }
    }
    
    public enum FailedBy: String {
        case failAll
    }
    
    public enum LockError: Error {
        case disposed(name: String, wasAcquiredAt: SourceLocation? = nil)
        case notAcquired(lock: AsyncAwaitLockMainActor)
        case prevented(lock: AsyncAwaitLockMainActor)
        case expresslyFailed(lock: AsyncAwaitLockMainActor, methodName: FailedBy)
        case timedOutWaiting(lock: AsyncAwaitLockMainActor, timeout: TimeInterval, acquiredAt: SourceLocation? = nil)
        case acquiredElsewhere(lock: AsyncAwaitLockMainActor, acquiredAt: SourceLocation? = nil)
        case replaced(lock: AsyncAwaitLockMainActor, by: SourceLocation? = nil)
    }
    
    public enum OnReplaced {
        case keepWaiting
        case resetTimeout
        case resume
        case throwReplaced
    }
    
    public enum OnTimeout {
        case resume
        case throwTimedOutWaiting
    }
    
    public nonisolated let name: String
    public nonisolated var description: String {
        name
    }
    
    public var isAcquired: Bool {
        acquiredLockID != nil
    }
    
    public private(set) var acquiredLockID: LockID? = nil
    
    static private let undefinedLockID: LockID = 0
    private var lockID: LockID = 0
    
    private var preventNewAcquires: Bool = false
    internal var continuationsAndLockIDsFIFO: [(
        continuation: CheckedContinuation<Void, any Error>,
        lockID: LockID,
        timeoutTask: Task<Void, Never>?,
        onReplaced: OnReplaced
    )] = []
    private var prematureReleaseLockIDs: Set<LockID> = []
    private var debugLockIDToSourceLocation: Dictionary<LockID, SourceLocation> = [:]
    
    private var waitAllWaitLock: AsyncAwaitLockMainActor? = nil

    
    // From Apple’s docs: The checked continuation offers detection of mis-use,
    // and dropping the last reference to it,
    // without having resumed it will trigger a warning.
    // Resuming a continuation twice is also diagnosed and will cause a crash.
    deinit {
        if continuationsAndLockIDsFIFO.count > 0 {
            print("WARNING: AsyncAwaitSemaphore.deinit: Lock named \(name) waiting continuations resumed by throwing .disposed error.")
        }
        
        let continuations = continuationsAndLockIDsFIFO
        continuationsAndLockIDsFIFO.removeAll()
        
        for (continuation, _, timeoutTask, _) in continuations {
            timeoutTask?.cancel()
            continuation.resume(throwing: LockError.disposed(name: name, wasAcquiredAt: debugLockIDToSourceLocation[acquiredLockID ?? Self.undefinedLockID]))
        }
        
        debugLockIDToSourceLocation.removeAll()
        
        // .release() for the acquired lock will be reached and will not throw.
        // However what will happen is unknown since deinit ran somehow (weak reference?).
        if acquiredLockID != nil {
            if name.hasSuffix("__waitAllWaitLock__") == false {
                print("WARNING: AsyncAwaitSemaphore.deinit: Lock named \(name) was still acquired when deinit ran.")
            }
            prematureReleaseLockIDs.insert(acquiredLockID!)
            acquiredLockID = nil
        }
    }
    
    
    // Same as deinit, except it can be called purposefully.
    private var disposed = false
    public func dispose() {
        disposeInternal(suppressLockedWarning: false)
    }
    internal func disposeInternal(suppressLockedWarning: Bool) {
        if continuationsAndLockIDsFIFO.count > 0 {
            print("WARNING: AsyncAwaitSemaphore.dispose(): Lock named \(name) waiting continuations resumed by throwing .disposed error.")
        }
        
        let continuations = continuationsAndLockIDsFIFO
        continuationsAndLockIDsFIFO.removeAll()
        
        for (continuation, _, timeoutTask, _) in continuations {
            timeoutTask?.cancel()
            continuation.resume(throwing: LockError.disposed(name: name, wasAcquiredAt: debugLockIDToSourceLocation[acquiredLockID ?? Self.undefinedLockID]))
        }
        
        debugLockIDToSourceLocation.removeAll()
        
        // .release() for the acquired lock will be reached and will not throw.
        // However what will happen is unknown since deinit ran somehow (weak reference?).
        if acquiredLockID != nil {
            if suppressLockedWarning == false {
                print("WARNING: AsyncAwaitSemaphore.dispose(): Lock named \(name) was still acquired when dispose() ran.")
            }
            prematureReleaseLockIDs.insert(acquiredLockID!)
            acquiredLockID = nil
        }
        
        waitAllWaitLock?.disposeInternal(suppressLockedWarning: true)
        waitAllWaitLock = nil
    }
    
    
    public init(name: String) {
        self.name = name
    }
    
    
    
    public func acquireNonWaiting() async throws -> LockID {
        return try await acquireNonWaiting(nil)
    }
    
    public func acquireNonWaiting(_ sourceLocation: (String, Int, Date)) async throws -> LockID {
        return try await acquireNonWaiting(SourceLocation(file: sourceLocation.0, line: sourceLocation.1, time: sourceLocation.2))
    }
    
    public func acquireNonWaiting(_ sourceLocation: SourceLocation? = nil) async throws -> LockID {
        if isAcquired {
            throw LockError.acquiredElsewhere(lock: self, acquiredAt: debugLockIDToSourceLocation[acquiredLockID!])
        }
        
        return try await acquire(sourceLocation)
    }
    
    
    
    public func acquire() async throws -> LockID {
        return try await acquire(nil)
    }
    
    public func acquire(
        timeout: TimeInterval? = nil,
        replaceWaiting: Bool = false,
        onReplaced: OnReplaced = .throwReplaced,
        onTimeout: OnTimeout = .throwTimedOutWaiting,
        _ sourceLocation: (String, Int, Date)
    ) async throws -> LockID {
        return try await acquire(
            timeout: timeout,
            replaceWaiting: replaceWaiting,
            onReplaced: onReplaced,
            onTimeout: onTimeout,
            SourceLocation(file: sourceLocation.0, line: sourceLocation.1, time: sourceLocation.2)
        )
    }
    
    public func acquire(
        timeout: TimeInterval? = nil,
        replaceWaiting: Bool = false,
        onReplaced: OnReplaced = .throwReplaced,
        onTimeout: OnTimeout = .throwTimedOutWaiting,
        _ sourceLocation: SourceLocation? = nil
    ) async throws -> LockID {
        var resetTimeout: Bool
        
        repeat {
            resetTimeout = false
            
            do {
                if disposed {
                    throw LockError.disposed(name: name)
                }
                
                if preventNewAcquires {
                    throw LockError.prevented(lock: self)
                }
                
                lockID += 1
                let lockID = lockID
                assert(lockID != Self.undefinedLockID)
                
                if isAcquired == false {
                    acquiredLockID = lockID
                }
                else {
                    if replaceWaiting {
                        let continuationsKeepWaiting = continuationsAndLockIDsFIFO.filter({
                            $0.onReplaced == .keepWaiting
                        })
                        let continuationsOther = continuationsAndLockIDsFIFO.filter({
                            $0.onReplaced != .keepWaiting
                        })
                        continuationsAndLockIDsFIFO.removeAll(keepingCapacity: true)
                        continuationsAndLockIDsFIFO.append(contentsOf: continuationsKeepWaiting)
                        
                        for (continuation, _, timeoutTask, _) in continuationsOther {
                            timeoutTask?.cancel()
                            continuation.resume(throwing: LockError.replaced(lock: self, by: debugLockIDToSourceLocation[lockID]))
                        }
                    }
                    
                    try await withCheckedThrowingContinuation { [self] (continuation: CheckedContinuation<Void, Error>) in
                        var sleepTask: Task<Void, Never>? = nil
                        
                        if timeout != nil {
                            sleepTask = Task { [self] in
                                try! await Task.sleep(nanoseconds: UInt64(1_000_000_000 * timeout!))
                                
                                let index = continuationsAndLockIDsFIFO.firstIndex(where: { $0.lockID == lockID })
                                if index != nil {
                                    continuationsAndLockIDsFIFO.remove(at: index!)
                                    continuation.resume(throwing: LockError.timedOutWaiting(lock: self, timeout: timeout!))
                                }
                            }
                        }
                        
                        continuationsAndLockIDsFIFO.append((
                            continuation: continuation,
                            lockID: lockID,
                            timeoutTask: sleepTask,
                            onReplaced: onReplaced
                        ))
                    }
                    
                    acquiredLockID = lockID
                }
                
                if sourceLocation != nil {
                    debugLockIDToSourceLocation[acquiredLockID!] = sourceLocation
                }
                
                return acquiredLockID!
            }
            catch {
                switch error as! LockError {
                case .timedOutWaiting:
                    switch onTimeout {
                    case .resume:
                        prematureReleaseLockIDs.insert(lockID)
                        return lockID
                    case .throwTimedOutWaiting:
                        throw error
                    }
                    
                case .replaced:
                    switch onReplaced {
                    case .keepWaiting:
                        fatalError(".keepWaiting should have been handled by keeping and not resuming the continuation.")
                    case .resetTimeout:
                        resetTimeout = true
                    case .resume:
                        prematureReleaseLockIDs.insert(lockID)
                        return lockID
                    case .throwReplaced:
                        throw error
                    }
                    
                default:
                    throw error
                    
                }
            }
        } while resetTimeout
        
        fatalError("Unreacheable code")
    }
    
    
    public func wait(
        timeout: TimeInterval? = nil,
        onReplaced: OnReplaced = .keepWaiting,
        onTimeout: OnTimeout = .throwTimedOutWaiting
    ) async throws {
        if isAcquired {
            var lockID: LockID = 0
            
            var wasReplaced: Bool
            
            repeat {
                if isAcquired == false {
                    return
                }
                
                wasReplaced = false
                
                do {
                    let lockOnReplaced: OnReplaced
                    switch onReplaced {
                    case .keepWaiting: lockOnReplaced = .keepWaiting
                    case .resetTimeout: lockOnReplaced = .throwReplaced
                    case .resume: lockOnReplaced = .throwReplaced
                    case .throwReplaced: lockOnReplaced = .throwReplaced
                    }
                    lockID = try await acquire(
                        timeout: timeout,
                        onReplaced: lockOnReplaced,
                        onTimeout: .throwTimedOutWaiting
                    )
                }
                catch {
                    switch error as! LockError {
                    case .timedOutWaiting:
                        switch onTimeout {
                        case .resume: return
                        case .throwTimedOutWaiting: throw error
                        }
                    case .replaced:
                        switch onReplaced {
                        case .resume: return
                        case .keepWaiting: fatalError(".keepWaiting should have been handled by keeping and not resuming the continuation.")
                        case .resetTimeout: wasReplaced = true
                        case .throwReplaced: throw error
                        }
                    default:
                        throw error
                    }
                }
            } while wasReplaced
            
            try! release(acquiredLockID: lockID)
        }
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
                throw LockError.acquiredElsewhere(lock: self, acquiredAt: debugLockIDToSourceLocation[self.acquiredLockID!])
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
            let (continuation, lockIDRemoved, timeoutTask, _) = continuationsAndLockIDsFIFO.removeFirst()
            debugLockIDToSourceLocation.removeValue(forKey: lockIDRemoved)
            timeoutTask?.cancel()
            
            continuation.resume()
        }
        else {
            self.acquiredLockID = nil
            debugLockIDToSourceLocation.removeAll(keepingCapacity: true)

            waitAllWaitLock?.resumeAllWaiting()
        }
    }
    
    
    public func whereAcquired() throws -> SourceLocation? {
        if isAcquired == false {
            throw LockError.notAcquired(lock: self)
        }
        
        return debugLockIDToSourceLocation[self.acquiredLockID!]
    }
    
    
    public func failNewAcquires() {
        preventNewAcquires = true
    }
    
    public func allowNewAcquires() {
        assert(disposed == false)
        
        preventNewAcquires = false
    }
    
    
    public func waitAll(timeout: TimeInterval? = nil, onTimeout: OnTimeout = .throwTimedOutWaiting) async throws {
        if disposed {
            throw LockError.disposed(name: name)
        }
        
        if isAcquired == false {
            assert(continuationsAndLockIDsFIFO.isEmpty)
            return
        }
        
        if waitAllWaitLock == nil {
            waitAllWaitLock = AsyncAwaitLockMainActor(name: "\(name)__waitAllWaitLock__")
            let _ = try await waitAllWaitLock!.acquireNonWaiting()
        }
        
        let _ = try await waitAllWaitLock!.acquire(timeout: timeout, onTimeout: onTimeout)
    }
    
    
    internal func resumeAllWaiting() {
        let continuations = continuationsAndLockIDsFIFO
        continuationsAndLockIDsFIFO.removeAll(keepingCapacity: true)
        debugLockIDToSourceLocation.removeAll(keepingCapacity: true)
        
        for (continuation, _, timeoutTask, _) in continuations {
            timeoutTask?.cancel()
            continuation.resume()
        }
    }
    
    
    internal func failAllInner(error: LockError, onlyWaiting: Bool) {
        waitAllWaitLock?.failAllInner(error: error, onlyWaiting: onlyWaiting)
        if onlyWaiting == false {
            waitAllWaitLock = nil
        }
        
        let continuations = continuationsAndLockIDsFIFO
        continuationsAndLockIDsFIFO.removeAll(keepingCapacity: true)
        debugLockIDToSourceLocation.removeAll(keepingCapacity: true)
        
        for (continuation, _, timeoutTask, _) in continuations {
            timeoutTask?.cancel()
            continuation.resume(throwing: error)
        }
        
        // .release() for the acquired lock will be reached and will not throw.
        if isAcquired && onlyWaiting == false {
            prematureReleaseLockIDs.insert(acquiredLockID!)
            acquiredLockID = nil
        }
    }
    
    
    public func failAll(onlyWaiting: Bool = false) throws {
        failAllInner(error: LockError.expresslyFailed(lock: self, methodName: .failAll), onlyWaiting: onlyWaiting)
        
        // if disposed == true {
            // return
        // }
    }
    
    
    public func checkReleased() throws {
        if isAcquired {
            throw LockError.acquiredElsewhere(lock: self, acquiredAt: debugLockIDToSourceLocation[self.acquiredLockID!])
        }
        
        assert(continuationsAndLockIDsFIFO.isEmpty)
        assert(debugLockIDToSourceLocation.isEmpty)
    }
}
