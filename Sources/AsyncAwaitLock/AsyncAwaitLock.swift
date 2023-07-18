import Foundation


public actor AsyncAwaitLock: CustomStringConvertible {
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
        case notAcquired(lock: AsyncAwaitLock)
        case prevented(lock: AsyncAwaitLock)
        case expresslyFailed(lock: AsyncAwaitLock, methodName: FailedBy)
        case timedOutWaiting(lock: AsyncAwaitLock, timeout: TimeInterval, acquiredAt: SourceLocation? = nil)
        case acquiredElsewhere(lock: AsyncAwaitLock, acquiredAt: SourceLocation? = nil)
        case replaced(lock: AsyncAwaitLock, by: SourceLocation? = nil)
        case taskCancelled(lock: AsyncAwaitLock, error: CancellationError)
    }
    
    public enum OnReplaced {
        case keepWaiting
        case resume
        case throwReplaced
    }
    
    public enum OnTimeout {
        case resume
        case throwTimedOutWaiting
    }
    
    public enum SourceLocationDebuggingMode {
        case enabledIfDebuggerPresent
        case enabled
        case disabled
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
    private var continuationsAndLockIDsFIFO: [(
        continuation: CheckedContinuation<Void, any Error>,
        lockID: LockID,
        timeoutTask: Task<Void, Never>?,
        onReplaced: OnReplaced
    )] = []
    private var prematureReleaseLockIDs: Set<LockID> = []
    private var enableSourceLocationDebugging: Bool
    private var debugLockIDToSourceLocation: Dictionary<LockID, SourceLocation> = [:]
    
    private var waitAllWaitLock: AsyncAwaitLock? = nil
    
    
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
    public func dispose() async {
        await disposeInternal(suppressLockedWarning: false)
    }
    internal func disposeInternal(suppressLockedWarning: Bool) async {
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
        
        await waitAllWaitLock?.disposeInternal(suppressLockedWarning: true)
        waitAllWaitLock = nil
    }
    
    
    public init(name: String, enableSourceLocationDebugging: SourceLocationDebuggingMode = .disabled) {
        self.name = name
        
        switch enableSourceLocationDebugging {
        case .enabledIfDebuggerPresent:
#if DEBUG
            self.enableSourceLocationDebugging = true
#else
            self.enableSourceLocationDebugging = false
#endif
        case .enabled:
            self.enableSourceLocationDebugging = true
        case .disabled:
            self.enableSourceLocationDebugging = false
        }
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
        // Reusing the sleep task to detect task cancellation.
        let timeout = timeout ?? TimeInterval.greatestFiniteMagnitude
        
        
        
        if disposed {
            fatalError("Attempted to lock disposed() lock named \(name)")
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
                
                for (continuation, replacedLockID, timeoutTask, onReplaced) in continuationsOther {
                    timeoutTask?.cancel()
                    switch onReplaced {
                    case .keepWaiting: fatalError("Unexpected .keepWaiting in array.")
                    case .resume:
                        prematureReleaseLockIDs.insert(replacedLockID)
                        continuation.resume()
                    case .throwReplaced: continuation.resume(throwing: LockError.replaced(lock: self, by: debugLockIDToSourceLocation[lockID]))
                    }
                }
            }
            
            try await withCheckedThrowingContinuation { [self] (continuation: CheckedContinuation<Void, Error>) in
                let sleepTask: Task<Void, Never> = Task { [self] in
                    let cancellationError: CancellationError?
                    do {
                        if timeout == TimeInterval.greatestFiniteMagnitude {
                            try await Task.sleep(nanoseconds: 1_000_000_000 * 200 * 365 * 86400)
                            fatalError("200 years have passed most unexpectedly.")
                        }
                        else {
                            try await Task.sleep(nanoseconds: UInt64(1_000_000_000 * timeout))
                        }
                        
                        cancellationError = nil
                    }
                    catch {
                        cancellationError = error as? CancellationError
                    }
                    
                    let index = continuationsAndLockIDsFIFO.firstIndex(where: { $0.lockID == lockID })
                    if index != nil {
                        continuationsAndLockIDsFIFO.remove(at: index!)
                        
                        if cancellationError != nil {
                            continuation.resume(
                                throwing: LockError.taskCancelled(
                                    lock: self,
                                    error: cancellationError!
                                )
                            )
                        }
                        else {
                            switch onTimeout {
                                
                            case .resume:
                                prematureReleaseLockIDs.insert(lockID)
                                continuation.resume()
                                
                            case .throwTimedOutWaiting:
                                continuation.resume(
                                    throwing: LockError.timedOutWaiting(
                                        lock: self,
                                        timeout: timeout,
                                        acquiredAt: debugLockIDToSourceLocation[acquiredLockID ?? Self.undefinedLockID]
                                    )
                                )
                            }
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
        
        if enableSourceLocationDebugging && sourceLocation != nil {
            debugLockIDToSourceLocation[acquiredLockID!] = sourceLocation
        }
        
        return acquiredLockID!
    }
    
    
    public func wait(
        timeout: TimeInterval? = nil,
        onReplaced: OnReplaced = .keepWaiting,
        onTimeout: OnTimeout = .throwTimedOutWaiting
    ) async throws {
        if isAcquired {
            let lockID = try await acquire(
                timeout: timeout,
                onReplaced: onReplaced,
                onTimeout: onTimeout
            )
            
            try! await release(acquiredLockID: lockID)
        }
    }
    
    
    public func release(acquiredLockID: LockID, ignoreRepeatRelease: Bool = false) async throws {
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
            
            await waitAllWaitLock?.resumeAllWaiting()
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
            waitAllWaitLock = AsyncAwaitLock(name: "\(name)__waitAllWaitLock__")
            let _ = try await waitAllWaitLock!.acquireNonWaiting()
        }
        
        let _ = try await waitAllWaitLock!.acquire(timeout: timeout, onTimeout: onTimeout)
        
        // See how resumeAllWaiting() doesn't store the prematurely released lock IDs,
        // as it is only used to undo this function, .waitAll()
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
    
    
    internal func failAllInner(error: LockError, onlyWaiting: Bool) async {
        await waitAllWaitLock?.failAllInner(error: error, onlyWaiting: onlyWaiting)
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
    
    public func failAll(onlyWaiting: Bool = false) async throws {
        await failAllInner(error: LockError.expresslyFailed(lock: self, methodName: .failAll), onlyWaiting: onlyWaiting)
        
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
