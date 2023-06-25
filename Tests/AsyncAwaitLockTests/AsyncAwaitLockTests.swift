import XCTest
import AsyncAwaitLock

final class AsyncAwaitLockTests: XCTestCase {
    func testExample() async throws {
        // Use XCTAssert and related functions to verify your tests produce the correct
        // results.
        
        let lock = AsyncAwaitLock(name: "Test")
        
        Task {
            print("Acquiring blocking lock A")
            let lockID = await lock.acquire(file: #filePath, line: #line)
            
            try! await Task.sleep(nanoseconds: 2_000_000_000)
            
            print("Releasing blocking lock A")
            try! await lock.release(acquiredLockID: lockID)
        }
        Task {
            print("Acquiring blocking lock B")
            let lockID = await lock.acquire(file: #filePath, line: #line)
            
            try! await Task.sleep(nanoseconds: 2_000_000_000)
            
            print("Releasing blocking lock B")
            try! await lock.release(acquiredLockID: lockID)
        }
        Task {
            try! await Task.sleep(nanoseconds: 500_000_000)
            print("Acquiring blocking lock C")
            let _ = await lock.acquire(file: #filePath, line: #line)
            
            // Not releasing here.
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
        try! await Task.sleep(nanoseconds: 6_000_000_000)
        
        // Unknown acquiredLockID release
        print("Releasing blocking lock C")
        try! await lock.release(acquiredLockID: nil)
        
        try! await lock.checkReleased()
        print("Released")
        
        // XCTAssertEqual(AsyncAwaitLock().text, "Hello, World!")
    }
}
