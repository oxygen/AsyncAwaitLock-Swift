// swift-tools-version: 5.8
// The swift-tools-version declares the minimum version of Swift required to build this package.

import PackageDescription

let package = Package(
    name: "AsyncAwaitLock",
    platforms: [
        .macOS(.v10_15),
        .iOS(.v13),
        .tvOS(.v13),
        .macCatalyst(.v13),
        .watchOS(.v9),
        .driverKit(.v22)
    ],
    products: [
        // Products define the executables and libraries a package produces, and make them visible to other packages.
        .library(
            name: "AsyncAwaitLock",
            targets: ["AsyncAwaitLock"]),
    ],
    dependencies: [
        // Dependencies declare other packages that this package depends on.
        // .package(url: /* package url */, from: "1.0.0"),
    ],
    targets: [
        // Targets are the basic building blocks of a package. A target can define a module or a test suite.
        // Targets can depend on other targets in this package, and on products in packages this package depends on.
        .target(
            name: "AsyncAwaitLock",
            dependencies: []
        ),
        .testTarget(
            name: "AsyncAwaitLockTests",
            dependencies: ["AsyncAwaitLock"]
        ),
    ]
)
