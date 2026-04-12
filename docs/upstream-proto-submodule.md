# Upstream Proto Submodule Management

This project uses a **sparse-checkout git submodule** of [apache/spark](https://github.com/apache/spark)
to keep proto definitions in sync with the upstream Spark Connect server.

Only the proto directory is checked out:
```
spark-upstream/sql/connect/common/src/main/protobuf/spark/connect/*.proto
```

## Current Configuration

| Item | Value |
|------|-------|
| Submodule path | `spark-upstream` |
| Remote URL | `https://github.com/apache/spark.git` |
| Branch | `branch-4.1` |
| Sparse checkout path | `sql/connect/common/src/main/protobuf/spark/connect` |
| sbt proto source | `baseDirectory.value / "spark-upstream" / "sql" / "connect" / "common" / "src" / "main" / "protobuf"` |

## Cloning the Repository (with submodule)

After cloning, initialize the submodule:

```bash
git clone https://github.com/LuciferYang/spark-connect-scala3.git
cd spark-connect-scala3
git submodule update --init
```

Then configure sparse checkout for the submodule (to avoid downloading the full Spark tree):

```bash
cd spark-upstream
git sparse-checkout init
git sparse-checkout set sql/connect/common/src/main/protobuf/spark/connect
cd ..
```

## Upgrading to a New Upstream Spark Version

When a new Spark version is released (e.g., upgrading from `branch-4.1` to `branch-4.2`):

### Step 1: Update the submodule branch

Edit `.gitmodules` to point to the new branch:

```ini
[submodule "spark-upstream"]
    path = spark-upstream
    url = https://github.com/apache/spark.git
    branch = branch-4.2
    shallow = true
```

### Step 2: Fetch and checkout the new branch

```bash
cd spark-upstream
git fetch origin branch-4.2
git checkout origin/branch-4.2
cd ..
```

### Step 3: Verify sparse checkout is still active

```bash
cd spark-upstream
git sparse-checkout list
# Should show: sql/connect/common/src/main/protobuf/spark/connect
ls sql/connect/common/src/main/protobuf/spark/connect/*.proto
cd ..
```

### Step 4: Compile and test

```bash
build/sbt compile
build/sbt test
```

### Step 5: Check for new proto fields

Compare the new proto definitions with the current codebase. In particular, check
if the upstream `catalog.proto` has added fields for operations that are currently
implemented via SQL fallback in `Catalog.scala`:

```bash
# Show diff between old and new catalog.proto
cd spark-upstream
git diff HEAD@{1} -- sql/connect/common/src/main/protobuf/spark/connect/catalog.proto
cd ..
```

SQL fallback methods to check (see TODO comments in `Catalog.scala`):
- `listViews` / `listPartitions` / `listCachedTables`
- `getTableProperties` / `getCreateTableString`
- `createDatabase` / `dropDatabase`
- `dropTable` / `dropView`
- `truncateTable` / `analyzeTable`

If upstream adds proto support for any of these, replace the SQL fallback with
proto-based implementation.

### Step 6: Commit the submodule update

```bash
git add spark-upstream .gitmodules
git commit -m "chore: upgrade upstream proto submodule to branch-4.2"
```

## Initial Setup (from scratch)

If you ever need to recreate the submodule from scratch:

```bash
# 1. Remove existing submodule
git submodule deinit -f spark-upstream
git rm -f spark-upstream
rm -rf .git/modules/spark-upstream

# 2. Clone with sparse checkout + shallow + blob filter
git clone --depth 1 --branch branch-4.1 --filter=blob:none --sparse \
    git@github.com:apache/spark.git spark-upstream

cd spark-upstream
git sparse-checkout set sql/connect/common/src/main/protobuf/spark/connect
cd ..

# 3. Register as submodule manually
#    Move .git directory into main repo's modules
mkdir -p .git/modules
mv spark-upstream/.git .git/modules/spark-upstream

#    Create .git file pointer
echo "gitdir: ../.git/modules/spark-upstream" > spark-upstream/.git

#    Add worktree path to module config
git config -f .git/modules/spark-upstream/config core.worktree ../../../spark-upstream

#    Register in main .git/config
git config submodule.spark-upstream.url "https://github.com/apache/spark.git"
git config submodule.spark-upstream.active true

#    Add submodule commit to index
git update-index --add --cacheinfo 160000,$(cd spark-upstream && git rev-parse HEAD),spark-upstream

# 4. Create .gitmodules
cat > .gitmodules << 'EOF'
[submodule "spark-upstream"]
	path = spark-upstream
	url = https://github.com/apache/spark.git
	branch = branch-4.1
	shallow = true
EOF

# 5. Verify
git submodule status
# Should show: <sha> spark-upstream (heads/branch-4.1)

# 6. Verify compilation
build/sbt compile
```

## Why Sparse Checkout?

The full apache/spark repository is ~3GB. Using sparse checkout with `--filter=blob:none`
reduces the submodule footprint to ~6MB (only proto files + git metadata), making clone
and CI times practical.

## CI Notes

In GitHub Actions, use `submodules: recursive` in the checkout action:

```yaml
- uses: actions/checkout@v4
  with:
    submodules: recursive
```

Since we use `shallow = true` in `.gitmodules`, the CI will only fetch a shallow
clone of the submodule. However, sparse checkout configuration needs to be applied
after clone — see the CI workflow for details.
