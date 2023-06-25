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
            }
            catch {
                switch error as? AsyncAwaitLock.LockError {
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
            }
            catch {
                switch error as? AsyncAwaitLock.LockError {
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
            }
            catch {
                switch error as? AsyncAwaitLock.LockError {
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
            }
            catch {
                switch error as? AsyncAwaitLock.LockError {
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
        
        // Plenty of time for those Tasks to execute.
        // Not the best kind of testing.
        try! await Task.sleep(nanoseconds: 8_000_000_000)
        
        try! await lock.checkReleased()
        print("Released")
        
        // XCTAssertEqual(AsyncAwaitLock().text, "Hello, World!")
    }
}
