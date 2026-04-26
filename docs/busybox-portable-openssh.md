# BusyBox Portable OpenSSH Test

This document describes the first-pass Kubernetes test for running a mounted portable OpenSSH server inside a `busybox:1.36` pod.

The chosen upstream source for this test is the official Portable OpenSSH release:

- `openssh-10.2p1`
- source index: [Portable OpenSSH release directory](https://cdn.openbsd.org/pub/OpenBSD/OpenSSH/portable/)
- release page: [Portable Release](https://www.openssh.org/portable.html)

The goal is narrow:

- keep the image itself minimal
- mount the portable OpenSSH binary instead of baking it into the image
- mount only the specific account files we need under `/etc`
- mount all of `/etc/ssh` read-only as one directory
- mount `/home` read-only
- keep `/var/run` and `/tmp` writable with `emptyDir`

This test is intentionally simpler than a production SSH design. It is meant to answer:

- does a mounted portable `sshd` run correctly in the BusyBox pod?
- do read-only `/etc` account files plus a read-only `/etc/ssh` tree work?
- do read-only home directories with pre-created `authorized_keys` work?
- can multiple users authenticate at the same time?
- can SSH port forwarding work without a writable shell home?

The repository now also includes a higher-level worked example for the multi-replica case:

- [portable-openssh-canonical-routing.md](./portable-openssh-canonical-routing.md)

That example builds on this sample and shows how canonical IPv6 identities let SSH users land on different replicas and still forward traffic end to end.

## Current Result

The repository now includes a validated build-and-smoke-test path for this setup:

- builder image: [portable-openssh-bundle.Dockerfile](../tools/portable-openssh-bundle.Dockerfile)
- build helper: [build-portable-openssh-bundle.ps1](../tools/build-portable-openssh-bundle.ps1)
- BusyBox smoke test: [test-portable-openssh-busybox.ps1](../tools/tests/openssh/test-portable-openssh-busybox.ps1)

What was verified on the Ubuntu Docker host:

- official `openssh-10.2p1` portable source can be compiled into a static bundle
- the resulting `sshd` binary runs inside `busybox:1.36`
- read-only `/etc/passwd`, read-only `/etc/group`, read-only `/etc/ssh`, and read-only `/home` work together
- two separate users (`demo1` and `demo2`) can authenticate successfully with mounted `authorized_keys`

What we learned from the validation:

- the tested `10.2p1` bundle must include privilege-separation support through `/var/empty`
- the config must not include `UsePAM no` when the binary is compiled without PAM support, because this build treats `UsePAM` as an unsupported option
- the account files must include an `sshd` user and group
- the Kubernetes pod cannot drop every capability; `sshd` needs a small runtime capability set for privilege separation and user switching

What we learned from the later multi-replica canonical-routing validation:

- `GatewayPorts clientspecified` is needed when reverse listeners must bind to a canonical IPv6 instead of only loopback
- the SSH session hook can call the allocator at login time to move a canonical IPv6 onto the accepting replica
- storing the hook inputs in `/var/run/ssh-canonical.env` is more reliable than assuming the user-session process will inherit the container environment directly

## Current Mount Model

The sample manifest is [busybox-portable-openssh-test.yaml](../k8s/busybox-portable-openssh-test.yaml).

The mounted paths are:

- `/seed-openssh`
  Read-only portable OpenSSH bundle from an external volume.
- `/opt/openssh`
  Writable `emptyDir` where the pod stages the portable OpenSSH bundle before launch.
- `/var/empty`
  Writable `emptyDir` prepared by the pod at startup for OpenSSH privilege separation.
- `/external-etc`
  Read-only external directory that contains `passwd`, `group`, and optionally `shadow`.
- `/etc/ssh`
  Read-only whole-directory mount from a projected volume that combines:
  - one ConfigMap for `sshd_config`
  - one Secret for SSH host keys
- `/opt/ssh-policy`
  Read-only policy-script mount used by `authorized_keys` forced-command profiles.
- `/home`
  Read-only external volume containing all user homes and `.ssh/authorized_keys`.
- `/var/run`
  Writable `emptyDir` for pid files and runtime sockets.
- `/tmp`
  Writable `emptyDir` for temporary files.

At container startup, the pod replaces the image-local account files with symlinks:

- `/etc/passwd -> /external-etc/passwd`
- `/etc/group -> /external-etc/group`
- `/etc/shadow -> /external-etc/shadow` when that file exists

This is a better fit than mounting all of `/etc`, because it avoids hiding unrelated image or Kubernetes-managed files such as `/etc/hosts` and `/etc/resolv.conf`, while still allowing the account files to update live through the external mount.

The sample now stages the OpenSSH bundle into writable runtime storage before `sshd` starts. That solves two practical problems that showed up on the local Docker Desktop testbed:

- the bundle arrived from Windows storage without executable bits
- the portable build expects helper binaries under the compiled `/opt/openssh/...` prefix

So the pod now:

1. mounts the external bundle read-only at `/seed-openssh`
2. copies it into writable `/opt/openssh`
3. restores executable permissions on the staged binaries
4. starts `sshd` from the staged `/opt/openssh` tree

The current sample keeps a narrow capability set instead of dropping everything. The validated minimum for this OpenSSH layout is:

- `DAC_OVERRIDE`
- `SETGID`
- `SETUID`
- `SYS_CHROOT`

Without these, the pod starts but the SSH handshake fails before authentication because `sshd` cannot complete privilege separation.

## Checklist Before Applying The Manifest

### 1. Portable OpenSSH bundle

Prepare one read-only external volume for the portable OpenSSH files.

The recommended source for the bundle is:

- compile `openssh-10.2p1` from the official Portable OpenSSH source

Why this is the best fit:

- it is the current official portable upstream release
- it avoids guessing about third-party BusyBox-focused repacks
- it lets us build a self-contained bundle specifically for this test

Required minimum:

- `sshd`

Nice to have:

- `ssh`
- `ssh-keygen`
- `sftp-server`

Expected staged tree inside the running pod:

- `/seed-openssh` as the read-only source
- `/opt/openssh` as the writable staged runtime

The manifest tries these `sshd` locations in order:

- `/opt/openssh/sbin/sshd`
- `/opt/openssh/bin/sshd`
- `/opt/openssh/sshd`

The first validation step is simply:

- the mounted `sshd` binary must be executable inside the BusyBox pod

The sample manifest assumes the external claim already exists and is populated before deployment:

- `portable-openssh-runtime`

The staged runtime tree is expected to contain:

```text
opt/openssh/
var/empty/
```

The sample manifest mounts the runtime claim read-only at:

- `opt/openssh -> /seed-openssh`

### 2. Account files directory

Prepare one read-only external directory mounted at:

- `/external-etc`

Required files:

- `passwd`
- `group`

Optional:

- `shadow`

The example users are:

- `sshd` as the privilege-separation account
- `demo1` with `uid:gid 1000:1000`
- `demo2` with `uid:gid 1001:1001`

The important rule is:

- the numeric `uid:gid` in `/etc/passwd` and `/etc/group` must match the ownership already applied on the mounted `/home` volume

The sample manifest assumes this claim already exists and is populated before deployment:

- `portable-openssh-etc`

### 3. SSH configuration directory

Prepare the full read-only `/etc/ssh` tree through the projected volume:

- ConfigMap `portable-openssh-etc`
- Secret `portable-openssh-host-keys`, created separately from real host-key material

Required content:

- `sshd_config`
- `ssh_host_ed25519_key`
- `ssh_host_ed25519_key.pub`

Optional:

- RSA host key pair if you want it

The sample manifest deliberately does not embed host keys. Create the Secret separately before applying the Deployment, for example from files generated by `ssh-keygen`.

The projected `/etc/ssh` volume should use mode `0600` so that the private host-key file is not exposed too broadly.

For this first pass:

- `PasswordAuthentication no`
- `PubkeyAuthentication yes`
- `PermitRootLogin no`
- `Port 2222`
- explicit `HostKey /etc/ssh/ssh_host_ed25519_key`

The sample keeps `AllowTcpForwarding yes` because the immediate goal is to verify that forwarding works.

### 4. Read-only home volume

Prepare one external read-only volume mounted at:

- `/home`

Example layout:

```text
/home/demo1
/home/demo1/.ssh
/home/demo1/.ssh/authorized_keys
/home/demo2
/home/demo2/.ssh
/home/demo2/.ssh/authorized_keys
```

Recommended permissions applied beforehand on the external volume:

- `/home/demo1`: `0750` or `0755`
- `/home/demo1/.ssh`: `0700`
- `/home/demo1/.ssh/authorized_keys`: `0600`
- same for `demo2`

Because this test does not expect interactive shell use, a read-only `/home` is acceptable. The home directory mainly exists for:

- SSH key lookup
- ownership and permission checks
- identity mapping

The sample manifest assumes this claim already exists and is populated before deployment:

- `portable-openssh-home`

If you want multiple pods to reuse the same content read-only, the backing storage must support that access pattern. In practice that usually means a storage class or external volume that can satisfy a shared read-only mount model.

### 5. Writable runtime directories

The manifest uses `emptyDir` for:

- `/var/run`
- `/tmp`

This is important because `sshd` still needs writable runtime state even when `/etc` and `/home` are read-only.

### 6. Privilege-separation directory

Portable OpenSSH requires privilege separation.

The official INSTALL notes that privilege separation is required and that the user, group, and directory used by `sshd` must exist.

For this test, the pod prepares `/var/empty` itself at startup and applies:

- owner `root:root`
- mode `0755`

That is more robust on the local Windows-backed test path than relying on pre-seeded PVC metadata for the privilege-separation directory.

### 7. First-start validation inside the pod

The sample startup command now does:

1. create `/etc/passwd`, `/etc/group`, and optional `/etc/shadow` symlinks
2. copy `/seed-openssh` into writable `/opt/openssh`
3. restore execute bits on the staged binaries
4. prepare `/var/empty` with the required ownership and mode
5. create `/var/run/sshd`
3. run:
   - `sshd -t -f /etc/ssh/sshd_config`
4. if validation passes, start:
   - `sshd -D -e -f /etc/ssh/sshd_config`

This makes config or permission issues visible immediately in the pod logs.

## Build Strategy

The repository should build a dedicated mounted bundle rather than relying on the host distro's `sshd`.

Recommended strategy:

1. download `openssh-10.2p1.tar.gz` from the official portable directory
2. build it in a controlled Linux environment
3. install it under a staging prefix such as `/opt/openssh`
4. publish the staged tree to the external read-only volume that the pod mounts

Practical target layout:

```text
opt/openssh/
  bin/
  sbin/sshd
  libexec/
var/empty/
```

The account files now live in a separate external tree, for example:

```text
passwd
group
shadow
```

That split is deliberate:

- the OpenSSH runtime bundle changes rarely
- account files may change often
- symlinking `/etc/passwd` and `/etc/group` into `/external-etc` lets new logins see account updates without a rollout

The main build customisations for this test should be:

- `--prefix=/opt/openssh`
- `--sysconfdir=/etc/ssh`
- `--without-pam`
- `--without-shadow`
- `--with-privsep-path=/var/empty`
- `--with-privsep-user=sshd`

Do not assume a third-party prebuilt "BusyBox OpenSSH" package is safe or current unless it can be traced back clearly. The official `10.2p1` portable source is the right baseline.

## What Could Still Be Problematic

### Portable binary compatibility

Even a "portable" build still has to work in the BusyBox pod. The first real gate is whether:

- the binary executes
- `sshd -t` passes

### OpenSSH privilege-separation expectations

The validated build expects:

- an `sshd` account in `/etc/passwd` and `/etc/group`
- `/var/empty` mounted read-only

If either of these is missing, the BusyBox pod should be treated as misconfigured.

### Live account-file updates

The symlink approach is specifically there to avoid `subPath` file mounts.

With `subPath`, running pods keep the old file until restart.

With:

- `/external-etc` mounted normally
- `/etc/passwd` and `/etc/group` symlinked to that mount at startup

new SSH connections can see updated account files without recreating the pod.

This does not remove the need to reload or restart `sshd` for every kind of change:

- account-file changes: should be visible to new logins
- `sshd_config` changes: reload or restart
- host-key changes: restart or careful rotation procedure

The dashboard-backed reconcile model for this sample is documented separately in:

- [portable-openssh-dashboard.md](./portable-openssh-dashboard.md)

Some OpenSSH builds expect additional paths such as:

- `/var/empty`

The current sample now prepares that path explicitly before `sshd` starts.

### Read-only homes

Read-only `/home` is acceptable for forwarding-only or non-interactive use, but it can still be limiting if later you want:

- shell history
- `known_hosts` updates
- user-created files

That is acceptable for this first test because the immediate focus is SSH auth plus forwarding, not interactive user sessions.

### Shared host keys

If multiple pods mount the same external `/etc/ssh` content, they will share host keys. That is acceptable for this test but should not be the final production identity model.

## Recommended Validation Order

1. Apply the manifest.
2. Check the pod logs.
3. Confirm `sshd -t` passed.
4. Confirm port `2222` is listening.
5. Test user `demo1` key login.
6. Test user `demo2` key login at the same time.
7. Verify local or remote port forwarding.
8. Only after that consider tightening the policy in `authorized_keys` or `sshd_config`.

## Why The Manifest Uses `/etc/ssh` As A Whole Directory

This is the best middle ground for the current test:

- simpler than individual file mounts for every SSH file
- safer than replacing all of `/etc`
- natural place to keep `sshd_config` and host keys together

So the final working assumption for this test is:

- `/etc/passwd` and `/etc/group` mounted individually
- `/etc/ssh` mounted as one read-only directory
- `/home` mounted read-only
- `/var/run` and `/tmp` writable
