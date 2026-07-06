// swift-tools-version: 5.9
//
// SPDX-FileCopyrightText: 2026 Stephen F. Booth <contact@sbooth.dev>
// SPDX-License-Identifier: MIT
//
// Part of https://github.com/sbooth/CXXMessageQueue
//

import PackageDescription

let package = Package(
    name: "CXXMessageQueue",
    products: [
        .library(
            name: "CXXMessageQueue",
            targets: [
                "CXXMessageQueue",
            ]
        ),
    ],
    targets: [
        .target(
            name: "CXXMessageQueue"
        ),
        .testTarget(
            name: "CXXMessageQueueTests",
            dependencies: [
                "CXXMessageQueue",
            ],
            swiftSettings: [
                .interoperabilityMode(.Cxx),
            ]
        ),
    ],
    cxxLanguageStandard: .cxx20
)
