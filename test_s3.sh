#!/bin/bash
# Drime S3 Gateway - Complete Test Suite
# Tests all supported S3 operations using AWS CLI

set -e

ENDPOINT="http://127.0.0.1:8081"
PROFILE="drime"
BUCKET="default"
TEST_DIR="test_$(date +%s)"
TEST_FILE="test_file.txt"
TEST_FILE2="test_file2.txt"

# Colors
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

echo "============================================"
echo "  Drime S3 Gateway - Complete Test Suite"
echo "============================================"
echo ""

# Helper functions
aws_s3() {
    aws --profile "$PROFILE" --endpoint-url "$ENDPOINT" s3 "$@"
}

aws_s3api() {
    aws --profile "$PROFILE" --endpoint-url "$ENDPOINT" s3api "$@"
}

test_pass() {
    echo -e "${GREEN}✓ $1${NC}"
}

test_fail() {
    echo -e "${RED}✗ $1${NC}"
}

# Create test files
mkdir -p /tmp/drime_test_local
echo "Hello from Drime S3 Gateway test!" > /tmp/drime_test_local/$TEST_FILE
echo "Second test file content" > /tmp/drime_test_local/$TEST_FILE2
echo "File for sync test" > /tmp/drime_test_local/sync_file.txt

echo -e "${BLUE}=== Basic Operations ===${NC}"
echo ""

echo -e "${YELLOW}[1/12] List Buckets${NC}"
if aws_s3 ls 2>/dev/null; then
    test_pass "List Buckets"
else
    test_fail "List Buckets"
fi
echo ""

echo -e "${YELLOW}[2/12] List Objects (root)${NC}"
if aws_s3 ls s3://$BUCKET/ 2>/dev/null; then
    test_pass "List Objects (root)"
else
    test_fail "List Objects (root)"
fi
echo ""

echo -e "${YELLOW}[3/12] PUT Object (s3 cp upload)${NC}"
if aws_s3 cp /tmp/drime_test_local/$TEST_FILE s3://$BUCKET/$TEST_DIR/$TEST_FILE 2>/dev/null; then
    test_pass "PUT Object"
else
    test_fail "PUT Object"
fi
echo ""

echo -e "${YELLOW}[4/12] List Objects (in folder)${NC}"
if aws_s3 ls s3://$BUCKET/$TEST_DIR/ 2>/dev/null; then
    test_pass "List Objects (folder)"
else
    test_fail "List Objects (folder)"
fi
echo ""

echo -e "${YELLOW}[5/12] HEAD Object${NC}"
if aws_s3api head-object --bucket $BUCKET --key "$TEST_DIR/$TEST_FILE" 2>/dev/null; then
    test_pass "HEAD Object"
else
    test_fail "HEAD Object"
fi
echo ""

echo -e "${YELLOW}[6/12] GET Object (download)${NC}"
rm -f /tmp/downloaded_test.txt
if aws_s3 cp s3://$BUCKET/$TEST_DIR/$TEST_FILE /tmp/downloaded_test.txt 2>/dev/null; then
    test_pass "GET Object"
else
    test_fail "GET Object"
fi
echo ""

echo -e "${YELLOW}[7/12] Verify Content Integrity${NC}"
if diff /tmp/drime_test_local/$TEST_FILE /tmp/downloaded_test.txt > /dev/null 2>&1; then
    test_pass "Content matches"
else
    test_fail "Content mismatch"
fi
echo ""

echo -e "${BLUE}=== Advanced Operations ===${NC}"
echo ""

echo -e "${YELLOW}[8/12] s3 mv (move/rename)${NC}"
if aws_s3 mv s3://$BUCKET/$TEST_DIR/$TEST_FILE s3://$BUCKET/$TEST_DIR/renamed_file.txt 2>/dev/null; then
    test_pass "s3 mv"
else
    test_fail "s3 mv"
fi
echo ""

echo -e "${YELLOW}[9/12] s3 cp (server to server copy)${NC}"
if aws_s3 cp s3://$BUCKET/$TEST_DIR/renamed_file.txt s3://$BUCKET/$TEST_DIR/copied_file.txt 2>/dev/null; then
    test_pass "s3 cp (server copy)"
else
    test_fail "s3 cp (server copy)"
fi
echo ""

echo -e "${YELLOW}[10/12] s3 sync (local → S3)${NC}"
if aws_s3 sync /tmp/drime_test_local s3://$BUCKET/$TEST_DIR/synced/ 2>/dev/null; then
    test_pass "s3 sync (upload)"
else
    test_fail "s3 sync (upload)"
fi
echo ""

echo -e "${YELLOW}[11/12] s3 sync (S3 → local)${NC}"
mkdir -p /tmp/drime_test_download
if aws_s3 sync s3://$BUCKET/$TEST_DIR/synced/ /tmp/drime_test_download 2>/dev/null; then
    test_pass "s3 sync (download)"
else
    test_fail "s3 sync (download)"
fi
echo ""

echo -e "${YELLOW}[12/12] DELETE Objects (cleanup)${NC}"
if aws_s3 rm s3://$BUCKET/$TEST_DIR/ --recursive 2>/dev/null; then
    test_pass "DELETE (recursive)"
else
    test_fail "DELETE (recursive)"
fi
echo ""

# Cleanup local files
rm -rf /tmp/drime_test_local /tmp/drime_test_download /tmp/downloaded_test.txt

echo "============================================"
echo -e "  ${GREEN}Test Suite Complete!${NC}"
echo "============================================"
