"""Starter for FND-03/01-raft-election. See README.md."""

from __future__ import annotations


def grant_vote(node, request):
    # TODO: apply Raft's vote rules in order; return (granted, new_node_state).
    raise NotImplementedError


def elect_leader(nodes, request):
    # TODO: ask every node, count grants, require a strict majority.
    raise NotImplementedError
