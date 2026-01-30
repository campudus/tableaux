# Tableaux ![Build Status](https://github.com/campudus/tableaux/actions/workflows/main_ci.yml/badge.svg?branch=master) [![Coverage Status](https://coveralls.io/repos/campudus/tableaux/badge.svg?branch=master&service=github)](https://coveralls.io/github/campudus/tableaux?branch=master) [![Codacy Badge](https://app.codacy.com/project/badge/Grade/07e1410aa5404dd29eaa0b569d55a6de)](https://www.codacy.com/gh/campudus/tableaux/dashboard?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=campudus/tableaux&amp;utm_campaign=Badge_Grade)

Tableaux (pronounced /ta.blo/) is a restful service for storing data in tables. These tables can have links between them.

## Getting Started

### Preparing the environment

To get a working setup, you need a jdd. The easiest way to work with different java versions is to use [jEnv](https://github.com/jenv/jenv). It also works out of the box with gradle. If jEnv doesn't pick up the locally configured JDK automatically you can use the following command to set it up manually:

```sh
jenv enable-plugin gradle
```

To check if gradle is working, you can use the following command, which will print all the infos and versions:

```sh
gradlew -v

> ------------------------------------------------------------
> Gradle 7.4.1
> ------------------------------------------------------------
> 
> Build time:   2022-03-09 15:04:47 UTC
> Revision:     36dc52588e09b4b72f2010bc07599e0ee0434e2e
> 
> Kotlin:       1.5.31
> Groovy:       3.0.9
> Ant:          Apache Ant(TM) version 1.10.11 compiled on July 10 2021
> JVM:          17.0.2 (Homebrew 17.0.2+0)
> OS:           Mac OS X 12.2.1 x86_64
```

### Setup

At first you need to setup your database and create a new `conf.json` based on `./conf-example.json`.
After that you can need to call `POST /system/reset` once to initialize system tables. If you wish you can fill in the demo data with `POST /system/resetDemo`.

### Update DB schema (optionally)

If you upgrade from an older schema version you need to call `POST /system/update` before that. Schema will be upgraded automatically.

## Auth

There are three different auth modes:

- 1. no auth (legacy)
- 2. manual auth with bearer token validation (JWT)
- 3. automatic keycloak auth discovery (JWT) - preferred

Auth modes 2. and 3. of Tableaux are secured by a JWT based authentication. The JWT (signed with a private key) is verified by the public key of the auth service. In manual auth mode 2. the public key is configured in the conf file (see `./conf-example-manual-auth.jsonc`), in automatic auth mode 3. the public key is discovered via the auth service also configured in the conf file (see `./conf-example.jsonc`)

The auth mode 1. is a legacy mode for testing or for running the service behind a different auth service. In this mode the incoming request is not verified. The user (e.g. for history entries) must be set via cookie `userName`. Legacy mode is activated, if `auth` key in config is missing.

## Build & Test

Tableaux uses gradle to build a so called **fat jar** which contains all runtime dependencies. You can find it in `build/libs/tableaux-fat.jar`. The gradle task `build` needs a running PostgreSQL and the `conf-test.json` must be configured correct. Requests in auth tests must contain an accessToken. For simplicity this accessToken is generated within a test helper with a hardcoded key pair. For the accessToken to match the pub key, the auth configuration for testing must always be the same as configured in `conf-test-example.json`.

```bash
./gradlew clean build
```

Build without running tests:

```bash
./gradlew clean assemble
```

## Tests (with custom config)

Tests use their own separate config, default configuration file is `conf-test.json`.
Specific config can be passed via arguments.

Run tests:

```bash
./gradlew test -Pconf='custom.json'
```

To run a single test use the following command:

```bash
# full package and test name or wildcard with *, e.g.:
./gradlew test --rerun-tasks --tests="*deleteTable_validRole*" --info"
```

## Run as fat jar

To execute the **fat jar** call it like this from project root:

```bash
java -jar ./build/libs/grud-backend-0.1.0-fat.jar -conf ../../conf.json
# with custom logging properties
java -jar -Djava.util.logging.config.file=./local_logging.properties ./build/libs/grud-backend-0.1.0-fat.jar -conf ./conf.json
```

## Run in development

```bash
./gradlew run
```

or with automatic redeploy on code changes

```bash
./gradlew runRedeploy
```

## Run with different `conf` file

```bash
./gradlew run -Pconf='other.json'
```

## Authentication and permission handling

TODO add documentation for authentication and permission handling. Currently the docs are filed in confluence and hackmd.io.

## Feature Flags

Feature flags are used to enable or disable certain features. They have to be configured in the configuration file. Feature flags are:

- `isRowPermissionCheckEnabled`: Enable or disable row permission checks (default: false)
- `isPublicFileServerEnabled`: Enable or disable the public file server. If enabled, files are accessible without authentication (default: false)

## Highlevel Features

- Content Creation System
- Content Translation System
- Digital Asset Management
- Editing Publishing Workflow
- Workspaces & Custom Projections

## Thumbnails

### Thumbnails configuration

Thumbnails for uploaded files can be generated/updated on server start.
Configuration for thumbnails and cache retention can be configured in the configuration file:

```json
"thumbnails": {
    // Resize filter used in thumbnail generation (value between 1 and 15) (default: 3 -> FILTER_TRIANGLE)
    "resizeFilter": 3,
    // Generate thumbnails at server start (default: false)
    "enableCacheWarmup": true,
    // Target widths for automatic thumbnail generation (default: [])
    "cacheWarmupWidths": [200, 400],
    // Chunks of thumbnails generated in parallel (default: 100)
    "cacheWarmupChunkSize": 100,
    // Maximum age of thumbnail in seconds before it is deleted (default: 2592000 -> 30 days)
    "cacheMaxAge": 2592000,
    // Polling interval in milliseconds for max age check (default: 21600000 -> 6 hours)
    "cacheClearPollingInterval": 21600000
}
```

### Thumbnails filter overview

| Int    | Constant Name            | Description                                                                                | Typical Use Cases & Performance                                                   |
| ------ | ------------------------ | ------------------------------------------------------------------------------------------ | --------------------------------------------------------------------------------- |
| **1**  | `FILTER_POINT`           | Nearest-neighbor interpolation – extremely fast but blocky and low quality.                | Best for real-time previews, thumbnails, or pixel art where sharp edges matter.   |
| **2**  | `FILTER_BOX`             | Box filter – averages nearby pixels; simple and efficient but can blur.                    | Good for quick downscaling with integer factors.                                  |
| **3**  | `FILTER_TRIANGLE` (**default**)   | Linear (bilinear) interpolation – smooths edges, moderate quality and speed.               | Default for many simple scaling tasks; good balance for most UIs.                 |
| **4**  | `FILTER_HERMITE`         | Hermite interpolation – smooth, continuous filter; slightly sharper than linear.           | Sometimes used for resizing smooth graphics or textures.                          |
| **5**  | `FILTER_HANNING`         | Hanning window filter – smooth windowed filter; suppresses ringing.                        | Used in scientific or high-fidelity image processing; slower than simple filters. |
| **6**  | `FILTER_HAMMING`         | Hamming window filter – similar to Hanning with slightly different weighting.              | Also used in high-fidelity image resampling or signal applications.               |
| **7**  | `FILTER_BLACKMAN`        | Blackman window filter – smooth filter with good frequency response and low aliasing.      | Ideal when reducing high-detail images; slower but very clean output.             |
| **8**  | `FILTER_GAUSSIAN`        | Gaussian blur filter – softens transitions, reduces aliasing.                              | Used when a smooth, natural look is preferred (e.g. photographic images).         |
| **9**  | `FILTER_QUADRATIC`       | Quadratic interpolation – smoother than bilinear, not as sharp as cubic.                   | Useful for moderate-quality resampling where performance matters.                 |
| **10** | `FILTER_CUBIC`           | Cubic interpolation – classic “bicubic” resampling with good sharpness.                    | Commonly used in photo editors; a good quality default.                           |
| **11** | `FILTER_CATROM`          | Catmull-Rom spline – sharp cubic filter preserving edges well.                             | Good for natural images where edge detail matters.                                |
| **12** | `FILTER_MITCHELL`        | Mitchell–Netravali cubic filter – balanced between sharpness and smoothness.               | Often used as a high-quality general-purpose resampler.                           |
| **13** | `FILTER_LANCZOS`         | Lanczos (windowed sinc) – excellent quality, minimal aliasing.                             | Best for downscaling photographs or detailed textures; slowest but sharpest.      |
| **14** | `FILTER_BLACKMAN_BESSEL` | Blackman–Bessel – very high-order smooth filter, minimal ringing.                          | Scientific or print applications where color fidelity is critical.                |
| **15** | `FILTER_BLACKMAN_SINC`   | Blackman–Sinc – Blackman window with Sinc kernel, extremely high quality.                  | Top-tier downscaling, archival or professional image processing; very slow.       |

## License

```txt
Copyright 2016-present Campudus GmbH.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
```
