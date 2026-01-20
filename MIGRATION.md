# Migration Guide

This document provides guidance for upgrading between major versions of `@dotdo/poc-graphdb`.

## v0.1.x to v0.2.x

No breaking changes expected.

This section will be updated with migration steps if breaking changes are introduced in v0.2.x.

## Checking for Breaking Changes

Before upgrading:

1. Review the [CHANGELOG.md](./CHANGELOG.md) for the target version
2. Search for entries marked with `BREAKING:` or `Breaking Change`
3. Run your test suite against the new version in a development environment
4. Check TypeScript compilation for any new type errors

## General Upgrade Process

1. Update the package version in your `package.json`
2. Run `npm install` to fetch the new version
3. Address any TypeScript compilation errors
4. Run your test suite
5. Review runtime behavior in a staging environment before production deployment
