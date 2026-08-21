ARG SWIFT_LINUX_BASE_IMAGE
FROM ${SWIFT_LINUX_BASE_IMAGE}

ARG SWIFT_LINUX_SNAPSHOT
ARG SWIFT_COMPILER_COMMIT
ARG SWIFT_LINUX_TOOLCHAIN_SHA256
ARG SQLITE_VERSION
ARG SQLITE_AMALGAMATION_URL
ARG SQLITE_AMALGAMATION_SHA256

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
      binutils \
      ca-certificates \
      clang \
      curl \
      git \
      libc6-dev \
      libcurl4-openssl-dev \
      libedit2 \
      libgcc-13-dev \
      libncurses5-dev \
      libpython3-dev \
      libsqlite3-0 \
      libstdc++-13-dev \
      libxml2-utils \
      libxml2-dev \
      libz3-dev \
      perl \
      pkg-config \
      tzdata \
      unzip \
      zlib1g-dev \
    && rm -rf /var/lib/apt/lists/*

RUN set -eux; \
    source_root="$(mktemp -d /tmp/sqlite-source.XXXXXX)"; \
    archive="$source_root/sqlite-amalgamation.zip"; \
    curl --connect-timeout 30 --max-time 1800 -fL --retry 3 \
      --output "$archive" "$SQLITE_AMALGAMATION_URL"; \
    printf '%s  %s\n' "$SQLITE_AMALGAMATION_SHA256" "$archive" \
      | sha256sum --check --strict; \
    unzip -q "$archive" -d "$source_root"; \
    source_directory="$(find "$source_root" -mindepth 1 -maxdepth 1 \
      -type d -name 'sqlite-amalgamation-*' -print -quit)"; \
    test -n "$source_directory"; \
    clang -O2 -fPIC -shared \
      -DSQLITE_THREADSAFE=1 \
      -DSQLITE_ENABLE_COLUMN_METADATA \
      -DSQLITE_ENABLE_FTS5 \
      -DSQLITE_ENABLE_RTREE \
      -DSQLITE_ENABLE_UNLOCK_NOTIFY \
      "$source_directory/sqlite3.c" \
      -ldl -lpthread -lm \
      -o /usr/local/lib/libsqlite3.so; \
    clang -O2 \
      -DSQLITE_THREADSAFE=1 \
      -DSQLITE_ENABLE_COLUMN_METADATA \
      -DSQLITE_ENABLE_FTS5 \
      -DSQLITE_ENABLE_RTREE \
      -DSQLITE_ENABLE_UNLOCK_NOTIFY \
      "$source_directory/shell.c" \
      "$source_directory/sqlite3.c" \
      -ldl -lpthread -lm \
      -o /usr/local/bin/sqlite3; \
    install -m 0644 "$source_directory/sqlite3.h" /usr/local/include/sqlite3.h; \
    install -m 0644 "$source_directory/sqlite3ext.h" /usr/local/include/sqlite3ext.h; \
    install -d -m 0755 /usr/local/lib/pkgconfig; \
    printf '%s\n' \
      'prefix=/usr/local' \
      'exec_prefix=${prefix}' \
      'libdir=${exec_prefix}/lib' \
      'includedir=${prefix}/include' \
      '' \
      'Name: SQLite' \
      'Description: SQL database engine' \
      "Version: $SQLITE_VERSION" \
      'Libs: -L${libdir} -lsqlite3' \
      'Libs.private: -ldl -lpthread -lm' \
      'Cflags: -I${includedir}' \
      > /usr/local/lib/pkgconfig/sqlite3.pc; \
    ldconfig; \
    rm -rf "$source_root"

ENV PKG_CONFIG_PATH=/usr/local/lib/pkgconfig
ENV LD_LIBRARY_PATH=/usr/local/lib

RUN set -eux; \
    install -d -m 0755 /usr/local/share/database-framework; \
    printf '%s\n' \
      "format=database-framework-sqlite-test-image-v1" \
      "swift_snapshot=$SWIFT_LINUX_SNAPSHOT" \
      "swift_compiler_commit=$SWIFT_COMPILER_COMMIT" \
      "swift_toolchain_sha256=$SWIFT_LINUX_TOOLCHAIN_SHA256" \
      "sqlite_version=$SQLITE_VERSION" \
      "sqlite_amalgamation_sha256=$SQLITE_AMALGAMATION_SHA256" \
      > /usr/local/share/database-framework/sqlite-test-image.env; \
    test "$(sqlite3 ':memory:' 'select sqlite_version();')" = "$SQLITE_VERSION"; \
    test "$(pkg-config --modversion sqlite3)" = "$SQLITE_VERSION"
