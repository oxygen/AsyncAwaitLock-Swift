import XCTest
import AsyncAwaitLock

final class AsyncAwaitLockTests: XCTestCase {
    func testExample() async throws {
        // Use XCTAssert and related functions to verify your tests produce the correct
        // results.
        
        let lock = AsyncAwaitLock(name: "Test")
        
        
        Task {
            print("Acquiring blocking lock O")
            
            let lockID: AsyncAwaitLock.LockID
            do {
                lockID = try await lock.acquire(file: #filePath, line: #line)
                print("Acquired lock O")
            }
            catch {
                switch error as! AsyncAwaitLock.LockError {
                case .replaced:
                    print("Lock O was replaced, exiting Task.")
                    return
                default: throw error
                }
            }
            
            try! await Task.sleep(nanoseconds: 2_000_000_000)
            
            print("Releasing blocking lock O")
            try! await lock.release(acquiredLockID: lockID)
        }
        Task {
            print("Acquiring blocking lock A")
            
            let lockID: AsyncAwaitLock.LockID
            do {
                lockID = try await lock.acquire(file: #filePath, line: #line)
                print("Acquired lock A")
            }
            catch {
                switch error as! AsyncAwaitLock.LockError {
                case .replaced:
                    print("Lock A was replaced, exiting Task.")
                    return
                default: throw error
                }
            }
            
            try! await Task.sleep(nanoseconds: 2_000_000_000)
            
            print("Releasing blocking lock A")
            try! await lock.release(acquiredLockID: lockID)
        }
        Task {
            try! await Task.sleep(nanoseconds: 250_000_000)
            
            print("Acquiring blocking lock B")
            let lockID: AsyncAwaitLock.LockID
            do {
                lockID = try await lock.acquire(file: #filePath, line: #line)
                print("Acquired lock B")
            }
            catch {
                switch error as! AsyncAwaitLock.LockError {
                case .replaced:
                    print("Lock B was replaced, exiting Task.")
                    return
                default: throw error
                }
            }
            assert(false) // Should have been replaced by C
            
            try! await Task.sleep(nanoseconds: 2_000_000_000)
            
            print("Releasing blocking lock B")
            try! await lock.release(acquiredLockID: lockID)
        }
        Task {
            try! await Task.sleep(nanoseconds: 500_000_000)
            print("Acquiring blocking lock C")
            let lockID: AsyncAwaitLock.LockID
            do {
                lockID = try await lock.acquire(replaceWaiting: true, file: #filePath, line: #line)
                print("Acquired lock C")
            }
            catch {
                switch error as! AsyncAwaitLock.LockError {
                case .replaced:
                    print("Lock C was replaced, exiting Task.")
                    return
                default: throw error
                }
            }
            
            try! await Task.sleep(nanoseconds: 2_000_000_000)
            
            print("Releasing blocking lock C")
            try! await lock.release(acquiredLockID: lockID)
        }
        Task {
            try! await Task.sleep(nanoseconds: 700_000_000)
            print("Acquiring blocking lock T with timeout")
            
            do {
                let _ = try await lock.acquire(timeout: 0.01, file: #filePath, line: #line)
                assert(false)
            }
            catch {
                switch error as! AsyncAwaitLock.LockError {
                case .notAcquired:
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
        
        try! await lock.checkReleased()
        print("Released all locks, everything clean.")
        print("-------------------------------------")
        print("                                     ")
        
        
        print("Testing .failAll()")
        
        Task {
            print("Acquiring blocking lock O")
            
            let lockID: AsyncAwaitLock.LockID
            do {
                lockID = try await lock.acquire(file: #filePath, line: #line)
                print("Acquired lock O")
            }
            catch {
                switch error as! AsyncAwaitLock.LockError {
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
            try! await lock.release(acquiredLockID: lockID)
        }
        Task {
            print("Acquiring blocking lock A")
            
            let lockID: AsyncAwaitLock.LockID
            do {
                lockID = try await lock.acquire(file: #filePath, line: #line)
                print("Acquired lock A")
            }
            catch {
                switch error as! AsyncAwaitLock.LockError {
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
            try! await lock.release(acquiredLockID: lockID)
        }
        try! await Task.sleep(nanoseconds: 500_000_000)
        print("Failling all waiting locks.")
        await lock.failNewAcquires()
        try! await lock.failAll()
        try! await lock.checkReleased()
        Task {
            await lock.dispose()
        }
        
        
        print("Released all locks, everything clean.")
        print("-------------------------------------")
        print("                                     ")
        
        
        // XCTAssertEqual(AsyncAwaitLock().text, "Hello, World!")
    }
}
