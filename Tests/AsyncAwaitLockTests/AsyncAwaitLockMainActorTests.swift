import XCTest
import AsyncAwaitLock

@MainActor
final class AsyncAwaitLockMainActorTests: XCTestCase {
    func testExample() async throws {
        // Use XCTAssert and related functions to verify your tests produce the correct
        // results.
        
        print("--------**** Begin MainActor **** -------")
        
        let lock = AsyncAwaitLockMainActor(name: "Test")
        
        
        Task {
            print("Acquiring blocking lock O")
            
            let lockID: AsyncAwaitLockMainActor.LockID
            do {
                lockID = try await lock.acquire(file: #filePath, line: #line)
                print("Acquired lock O")
            }
            catch {
                switch error as! AsyncAwaitLockMainActor.LockError {
                case .replaced:
                    print("Lock O was replaced, exiting Task.")
                    return
                default: throw error
                }
            }
            
            try! await Task.sleep(nanoseconds: 2_000_000_000)
            
            print("Releasing blocking lock O")
            try! lock.release(acquiredLockID: lockID)
        }
        Task {
            print("Acquiring blocking lock A")
            
            let lockID: AsyncAwaitLockMainActor.LockID
            do {
                lockID = try await lock.acquire(file: #filePath, line: #line)
                print("Acquired lock A")
            }
            catch {
                switch error as! AsyncAwaitLockMainActor.LockError {
                case .replaced:
                    print("Lock A was replaced, exiting Task.")
                    return
                default: throw error
                }
            }
            
            try! await Task.sleep(nanoseconds: 2_000_000_000)
            
            print("Releasing blocking lock A")
            try! lock.release(acquiredLockID: lockID)
        }
        Task {
            try! await Task.sleep(nanoseconds: 250_000_000)
            
            print("Acquiring blocking lock B")
            let lockID: AsyncAwaitLockMainActor.LockID
            do {
                lockID = try await lock.acquire(file: #filePath, line: #line)
                print("Acquired lock B")
            }
            catch {
                switch error as! AsyncAwaitLockMainActor.LockError {
                case .replaced:
                    print("Lock B was replaced, exiting Task.")
                    return
                default: throw error
                }
            }
            assert(false) // Should have been replaced by C
            
            try! await Task.sleep(nanoseconds: 2_000_000_000)
            
            print("Releasing blocking lock B")
            try! lock.release(acquiredLockID: lockID)
        }
        Task {
            try! await Task.sleep(nanoseconds: 500_000_000)
            print("Acquiring blocking lock C")
            let lockID: AsyncAwaitLockMainActor.LockID
            do {
                lockID = try await lock.acquire(replaceWaiting: true, file: #filePath, line: #line)
                print("Acquired lock C")
            }
            catch {
                switch error as! AsyncAwaitLockMainActor.LockError {
                case .replaced:
                    print("Lock C was replaced, exiting Task.")
                    return
                default: throw error
                }
            }
            
            try! await Task.sleep(nanoseconds: 2_000_000_000)
            
            print("Releasing blocking lock C")
            try! lock.release(acquiredLockID: lockID)
        }
        Task {
            try! await Task.sleep(nanoseconds: 700_000_000)
            print("Acquiring blocking lock T with timeout")
            
            do {
                let _ = try await lock.acquire(timeout: 0.01, file: #filePath, line: #line)
                assert(false)
            }
            catch {
                switch error as! AsyncAwaitLockMainActor.LockError {
                case .timedOutWaiting:
                    print("Lock T timed out as expected.")
                    return
                default:
                    print("ERROR: Lock T dind't time out as expected.")
                    throw error
                }
            }
        }
        Task {
            // Wait for acquire in the previous tasks.
            try! await Task.sleep(nanoseconds: 1_000_000_000)
            
            do {
                let _ = try await lock.acquireNonWaiting(file: #filePath, line: #line)
                assert(false)
            }
            catch {
                print("nonWaiting works as expected.", error)
            }
        }
        
        try! await Task.sleep(nanoseconds: 1_000_000_000)
        try! await lock.waitAll()
        
        try! lock.checkReleased()
        print("Released all locks, everything clean.")
        print("-------------------------------------")
        print("                                     ")
        
        
        print("Testing .failAll()")
        
        Task {
            print("Acquiring blocking lock O")
            
            let lockID: AsyncAwaitLockMainActor.LockID
            do {
                lockID = try await lock.acquire(file: #filePath, line: #line)
                print("Acquired lock O")
            }
            catch {
                switch error as! AsyncAwaitLockMainActor.LockError {
                case .replaced:
                    print("Lock O was replaced, exiting Task.")
                    return
                default:
                    print("Lock O threw error:", error)
                    throw error
                }
            }
            
            try! await Task.sleep(nanoseconds: 2_000_000_000)
            
            print("Releasing blocking lock O")
            try! lock.release(acquiredLockID: lockID)
        }
        Task {
            print("Acquiring blocking lock A")
            
            let lockID: AsyncAwaitLockMainActor.LockID
            do {
                lockID = try await lock.acquire(file: #filePath, line: #line)
                print("Acquired lock A")
            }
            catch {
                switch error as! AsyncAwaitLockMainActor.LockError {
                case .replaced:
                    print("Lock A was replaced, exiting Task.")
                    return
                default:
                    print("Lock A threw error:", error)
                    throw error
                }
            }
            
            try! await Task.sleep(nanoseconds: 2_000_000_000)
            
            print("Releasing blocking lock A")
            try! lock.release(acquiredLockID: lockID)
        }
        try! await Task.sleep(nanoseconds: 500_000_000)
        print("Failling all waiting locks.")
        lock.failNewAcquires()
        try! lock.failAll()
        try! lock.checkReleased()
        Task {
            lock.dispose()
        }
        
        
        print("Released all locks, everything clean.")
        print("-------------------------------------")
        print("                                     ")
        
        
        print("--------**** End MainActor **** -------")
        
        // XCTAssertEqual(AsyncAwaitLockMainActor().text, "Hello, World!")
    }
}
