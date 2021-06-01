# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.1.1] - 2021-06-01
### Fixed
- #153: AsyncResult is not serializable to json - thanks to @vlagorsse

## [1.1.0] - 2019-07-18
### Fixed
- CLI executor now properly handles retrieval of exceptions for failed tasks

### Removed
- Old Raven client for Sentry integration substituted with Sentry-SDK

### Added
- Configuration of Sentry using environment variables
- Added a new event emitted by message producer - FLOW_SCHEDULE
- Shiped adapters now support configuration based on environment variables
- This changelog file to track notable release changes

## [1.0.0] - 2018-08-01
