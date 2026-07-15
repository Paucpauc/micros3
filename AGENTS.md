# AGENTS.md — micros3

## Release / Push Tag

Steps to publish a new release tag:

1. **Verify build and tests pass:**
   ```bash
   go build ./...
   go vet ./...
   go test ./...
   ```

2. **Commit all changes:**
   ```bash
   git add -A
   git commit -m "<type>(<scope>): <description>"
   ```

3. **Create a tag** (use semantic versioning, e.g. `v0.3.4`):
   ```bash
   git tag v0.3.4
   ```

4. **Pull rebase from remote** (in case other commits were pushed):
   ```bash
   git pull --rebase origin main
   ```

5. **Push commits and tags:**
   ```bash
   git push origin main
   git push origin v0.3.4
   ```
   Or in one command:
   ```bash
   git push origin main --tags
   ```

6. **Verify** the GitHub Actions workflow triggered by the tag:
   - Check the Actions tab on GitHub for the docker-image build.
