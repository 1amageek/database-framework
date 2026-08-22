ARG SWIFT_LINUX_BASE_IMAGE=docker.io/library/ubuntu@sha256:b17516cd982bf06bdd5d5600253d12a8de017b9eb831cc052b532a0363d294f9
FROM ${SWIFT_LINUX_BASE_IMAGE}

ARG TEST_IMAGE_FINGERPRINT
ARG SWIFT_SNAPSHOT
ARG SWIFT_COMPILER_COMMIT
ARG SWIFT_LINUX_TOOLCHAIN_SHA256
ARG UBUNTU_PACKAGE_LOCK_SHA256
ARG FOUNDATIONDB_VERSION
ARG FOUNDATIONDB_LINUX_CLIENT_URL
ARG FOUNDATIONDB_LINUX_CLIENT_SHA256
ARG SQLITE_VERSION
ARG SQLITE_AMALGAMATION_URL
ARG SQLITE_AMALGAMATION_SHA256

ENV DEBIAN_FRONTEND=noninteractive

COPY ubuntu-packages.lock /tmp/database-framework-ubuntu-packages.lock

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
      binutils \
      ca-certificates \
      clang \
      curl \
      git \
      jq \
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
      netcat-openbsd \
      perl \
      pkg-config \
      postgresql-client \
      procps \
      rsync \
      tzdata \
      unzip \
      zlib1g-dev \
    && dpkg-query -W -f='${binary:Package}=${Version}\n' \
      | LC_ALL=C sort > /tmp/database-framework-ubuntu-packages.actual \
    && diff -u \
      /tmp/database-framework-ubuntu-packages.lock \
      /tmp/database-framework-ubuntu-packages.actual \
    && test "$(sha256sum /tmp/database-framework-ubuntu-packages.lock | awk '{ print $1 }')" \
      = "$UBUNTU_PACKAGE_LOCK_SHA256" \
    && install -d -m 0755 /usr/local/share/database-framework \
    && install -m 0644 /tmp/database-framework-ubuntu-packages.lock \
      /usr/local/share/database-framework/ubuntu-packages.lock \
    && rm -rf /var/lib/apt/lists/* \
      /tmp/database-framework-ubuntu-packages.actual \
      /tmp/database-framework-ubuntu-packages.lock

RUN set -eux; \
    client_package="$(mktemp /tmp/foundationdb-client.XXXXXX.deb)"; \
    curl --connect-timeout 30 --max-time 1800 -fL --retry 3 \
      --output "$client_package" "$FOUNDATIONDB_LINUX_CLIENT_URL"; \
    printf '%s  %s\n' "$FOUNDATIONDB_LINUX_CLIENT_SHA256" "$client_package" \
      | sha256sum --check --strict; \
    dpkg-deb --extract "$client_package" /; \
    rm -f "$client_package"; \
    test -x /usr/bin/fdbcli; \
    test -f /usr/include/foundationdb/fdb_c.h; \
    test -f /usr/lib/libfdb_c.so; \
    install -d -m 0755 /usr/local/lib/pkgconfig; \
    printf '%s\n' \
      'prefix=/usr' \
      'exec_prefix=${prefix}' \
      'libdir=${exec_prefix}/lib' \
      'includedir=${prefix}/include' \
      '' \
      'Name: libfdb' \
      'Description: FoundationDB C client' \
      "Version: $FOUNDATIONDB_VERSION" \
      'Libs: -L${libdir} -lfdb_c' \
      'Cflags: -I${includedir}' \
      > /usr/local/lib/pkgconfig/libfdb.pc; \
    fdbcli --version | grep -F "$FOUNDATIONDB_VERSION"

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
ENV LD_LIBRARY_PATH=/usr/local/lib:/usr/lib

RUN set -eux; \
    install -d -m 0755 /usr/local/share/database-framework; \
    printf '%s\n' \
      'format=database-framework-backend-test-image-v1' \
      "fingerprint=$TEST_IMAGE_FINGERPRINT" \
      "swift_snapshot=$SWIFT_SNAPSHOT" \
      "swift_compiler_commit=$SWIFT_COMPILER_COMMIT" \
      "swift_toolchain_sha256=$SWIFT_LINUX_TOOLCHAIN_SHA256" \
      "ubuntu_package_lock_sha256=$UBUNTU_PACKAGE_LOCK_SHA256" \
      "foundationdb_version=$FOUNDATIONDB_VERSION" \
      "foundationdb_client_sha256=$FOUNDATIONDB_LINUX_CLIENT_SHA256" \
      "sqlite_version=$SQLITE_VERSION" \
      "sqlite_amalgamation_sha256=$SQLITE_AMALGAMATION_SHA256" \
      > /usr/local/share/database-framework/backend-test-image.env; \
    test "$(sqlite3 ':memory:' 'select sqlite_version();')" = "$SQLITE_VERSION"; \
    test "$(pkg-config --modversion sqlite3)" = "$SQLITE_VERSION"

LABEL org.opencontainers.image.title="database-framework backend test runner" \
      org.opencontainers.image.description="Pinned Linux runner for SQLite, PostgreSQL, and FoundationDB integration tests" \
      database-framework.test-image.fingerprint="${TEST_IMAGE_FINGERPRINT}"
