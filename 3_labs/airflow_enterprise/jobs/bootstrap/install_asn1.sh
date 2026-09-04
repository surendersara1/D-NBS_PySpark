#!/bin/bash
#
# install_asn1.sh — EMR on EC2 bootstrap action for DAG 01's cluster.
#
# Referenced from JOB_FLOW_OVERRIDES["BootstrapActions"] in
# dags/01_cdr_mediation_hourly.py.
#
#   aws s3 cp install_asn1.sh s3://telco-prod-emr-code/bootstrap/
#
# WHY THIS FILE IS THE REASON DAG 01 USES EMR ON EC2 AT ALL
#
#   Raw call detail records arrive as ASN.1 BER, a binary telecom encoding that
#   no Spark reader understands. Decoding needs a native shared library on
#   every node before Spark starts.
#
#   Bootstrap actions and custom AMIs exist ONLY on EMR on EC2. EMR Serverless
#   has no node for you to prepare, and MWAA workers cannot install system
#   packages at all. When a workload genuinely needs the machine, that decides
#   the deployment model — not preference. Every other DAG in this lab uses
#   Serverless or EKS precisely because none of them needs this.
#
# BOOTSTRAP ACTION RULES
#   * runs as user `hadoop` on EVERY node, before Hadoop and Spark start
#   * a non-zero exit FAILS THE WHOLE CLUSTER, so failures must be explicit
#     rather than swallowed — a half-provisioned node that silently continues
#     produces a Spark job that dies confusingly 20 minutes later
#   * it must be idempotent: nodes added later by a resize run it again
#   * keep it under a couple of minutes; it is on the critical path of every
#     hourly run

set -euo pipefail

VERSION="4.2.1"
while [ $# -gt 0 ]; do
  case "$1" in
    --version) VERSION="$2"; shift 2 ;;
    *) echo "unknown argument: $1" >&2; exit 1 ;;
  esac
done

CODE_BUCKET="${CODE_BUCKET:-telco-prod-emr-code}"
PKG="asn1-cdr-decoder-${VERSION}"
S3_URI="s3://${CODE_BUCKET}/native/${PKG}.tar.gz"
INSTALL_DIR="/opt/telco/asn1"
MARKER="${INSTALL_DIR}/.installed-${VERSION}"

log() { echo "[bootstrap $(date -u +%H:%M:%S)] $*"; }

# Idempotent: a resize re-runs this on the new node only.
if [ -f "$MARKER" ]; then
  log "asn1 decoder ${VERSION} already present, nothing to do"
  exit 0
fi

log "installing asn1 decoder ${VERSION} from ${S3_URI}"

sudo mkdir -p "$INSTALL_DIR"
sudo chown -R hadoop:hadoop /opt/telco

# System build deps. yum on Amazon Linux 2023, which EMR 7.x uses.
sudo yum install -y --quiet libffi-devel openssl-devel gcc-c++ make \
  || { log "FATAL: yum install failed"; exit 1; }

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

aws s3 cp "$S3_URI" "$TMP/${PKG}.tar.gz" \
  || { log "FATAL: could not fetch ${S3_URI} — check the instance profile can read the bucket"; exit 1; }

# Verify before executing anything from the archive.
if aws s3 cp "s3://${CODE_BUCKET}/native/${PKG}.tar.gz.sha256" "$TMP/expected.sha256" 2>/dev/null; then
  ( cd "$TMP" && echo "$(cat expected.sha256)  ${PKG}.tar.gz" | sha256sum -c - ) \
    || { log "FATAL: checksum mismatch on ${PKG}.tar.gz"; exit 1; }
  log "checksum verified"
else
  log "WARNING: no published checksum for ${PKG}.tar.gz"
fi

tar -xzf "$TMP/${PKG}.tar.gz" -C "$INSTALL_DIR" --strip-components=1
sudo ldconfig "$INSTALL_DIR/lib"

# Make the library visible to the Python workers Spark launches.
cat <<EOF | sudo tee /etc/profile.d/telco-asn1.sh > /dev/null
export LD_LIBRARY_PATH="\${LD_LIBRARY_PATH:-}:${INSTALL_DIR}/lib"
export ASN1_SCHEMA_DIR="${INSTALL_DIR}/schemas"
EOF
sudo chmod 0644 /etc/profile.d/telco-asn1.sh

# The Python binding the mediation job imports.
sudo python3 -m pip install --quiet --no-input "$INSTALL_DIR/python/asn1_cdr-${VERSION}-py3-none-any.whl" \
  || { log "FATAL: pip install of the python binding failed"; exit 1; }

# Prove it works HERE rather than discovering it inside a Spark executor.
LD_LIBRARY_PATH="${INSTALL_DIR}/lib" python3 -c \
  "import asn1_cdr; print('asn1_cdr', asn1_cdr.__version__)" \
  || { log "FATAL: asn1_cdr import failed after install"; exit 1; }

touch "$MARKER"
log "asn1 decoder ${VERSION} installed successfully"
