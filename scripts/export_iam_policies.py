#!/usr/bin/env python3
"""Export the least-privilege IAM design from iac/01-foundation.yaml as a standalone JSON document
(iac/iam/pipeline-policies.json). The CloudFormation template stays the single source of truth; this file is the
reviewable artifact the course brief asks for. Placeholders: <account-id>, <region>, <project>, <data-bucket>.

    python3 scripts/export_iam_policies.py            # rewrite iac/iam/pipeline-policies.json
    python3 scripts/export_iam_policies.py --check    # exit 1 if the committed file is out of date
"""
import json
import re
import sys
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parent.parent
TEMPLATE = ROOT / "iac" / "01-foundation.yaml"
OUT = ROOT / "iac" / "iam" / "pipeline-policies.json"

SUBST = {
    "AWS::Partition": "aws",
    "AWS::Region": "<region>",
    "AWS::AccountId": "<account-id>",
    "ProjectName": "<project>",
    "DataBucket.Arn": "arn:aws:s3:::<data-bucket>",
}
REFS = {"AlertsTopic": "arn:aws:sns:<region>:<account-id>:<project>-pipeline-alerts"}


class Tag:
    def __init__(self, name, value):
        self.name, self.value = name, value


def construct(loader, suffix, node):
    if isinstance(node, yaml.ScalarNode):
        value = loader.construct_scalar(node)
    elif isinstance(node, yaml.SequenceNode):
        value = loader.construct_sequence(node, deep=True)
    else:
        value = loader.construct_mapping(node, deep=True)
    return Tag(suffix, value)


yaml.SafeLoader.add_multi_constructor("!", construct)


def resolve(node):
    if isinstance(node, Tag):
        if node.name == "Sub":
            return re.sub(r"\$\{([^}]+)\}", lambda m: SUBST[m.group(1)], node.value)
        if node.name == "GetAtt":
            return SUBST[node.value]
        if node.name == "Ref":
            return REFS[node.value]
        raise ValueError(f"unsupported tag !{node.name}")
    if isinstance(node, dict):
        return {k: resolve(v) for k, v in node.items()}
    if isinstance(node, list):
        return [resolve(v) for v in node]
    return node


def build():
    resources = yaml.safe_load(TEMPLATE.read_text())["Resources"]
    roles = {}
    for logical, res in resources.items():
        if res["Type"] != "AWS::IAM::Role":
            continue
        props = res["Properties"]
        trust = resolve(props["AssumeRolePolicyDocument"])
        roles[resolve(props["RoleName"])] = {
            "logicalId": logical,
            "trustedServices": sorted(
                {s for st in trust["Statement"] for s in ([st["Principal"]["Service"]] if isinstance(st["Principal"]["Service"], str) else st["Principal"]["Service"])}
            ),
            "managedPolicies": resolve(props.get("ManagedPolicyArns", [])),
            "inlinePolicies": {p["PolicyName"]: resolve(p["PolicyDocument"]) for p in props.get("Policies", [])},
        }
    return {
        "_description": "Least-privilege IAM design for the Kestrion CDC pipeline, generated from iac/01-foundation.yaml. "
        "One role per service; no role can reach another service's prefixes. Not hand-edited.",
        "_placeholders": ["<account-id>", "<region>", "<project>", "<data-bucket>"],
        "roles": roles,
    }


def main():
    text = json.dumps(build(), indent=2) + "\n"
    if "--check" in sys.argv:
        if not OUT.exists() or OUT.read_text() != text:
            print("iac/iam/pipeline-policies.json is out of date: run scripts/export_iam_policies.py")
            return 1
        print("iac/iam/pipeline-policies.json is up to date")
        return 0
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(text)
    print(f"Wrote {OUT.relative_to(ROOT)} ({len(build()['roles'])} roles)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
