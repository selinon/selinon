#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ######################################################################
# Copyright (C) 2016-2018  Fridolin Pokorny, fridolin.pokorny@gmail.com
# This file is part of Selinon project.
# ######################################################################

import pytest
from selinon.selective import compute_selective_run
from selinon_test_case import SelinonTestCase
from selinon import SelectiveNoPathError


class TestSelective(SelinonTestCase):
    @classmethod
    def _lists2sets(cls, d):
        for k, v in d.items():
            if isinstance(v, dict):
                d[k] = cls._lists2sets(v)
            elif isinstance(v, list) or isinstance(v, tuple):
                d[k] = set(v)
            else:
                d[k] = v
        return d

    def test_compute_selective_run_simple(self):
        #
        # flow1:
        #         |     |
        #        T1    T2
        #       / |     |
        #      /   -----
        #     /      |
        #   T4       T3
        #    |       | \
        #     -------   \
        #        |       T6
        #       T5
        #
        edge_table = {
            'flow1': [{'from': [], 'to': ['Task1', 'Task2'], 'condition': self.cond_true},
                      {'from': ['Task1', 'Task2'], 'to': ['Task3'], 'condition': self.cond_true},
                      {'from': ['Task1'], 'to': ['Task4'], 'condition': self.cond_true},
                      {'from': ['Task3', 'Task4'], 'to': ['Task5'], 'condition': self.cond_true},
                      {'from': ['Task3'], 'to': ['Task6'], 'condition': self.cond_true}]
        }
        self.init(edge_table)
        selective = compute_selective_run('flow1', ['Task4', 'Task3'], follow_subflows=False, run_subsequent=False)
        self._lists2sets(selective)

        assert selective['waiting_edges_subset'] == {'flow1': {0: {'Task1', 'Task2'}, 1: {'Task3'}, 2: {'Task4'}}}
        assert set(selective['task_names']) == {'Task4', 'Task3'}

    def test_compute_selective_run_cyclic(self):
        #
        # flow1:
        #         |     |
        #        T1    T2 <-
        #         |     |   |
        #        T3    T4---
        #
        edge_table = {
            'flow1': [{'from': [], 'to': ['Task1', 'Task2'], 'condition': self.cond_true},
                      {'from': ['Task1'], 'to': ['Task3'], 'condition': self.cond_true},
                      {'from': ['Task2'], 'to': ['Task4'], 'condition': self.cond_true},
                      {'from': ['Task4'], 'to': ['Task2'], 'condition': self.cond_true}]
        }
        self.init(edge_table)
        selective = compute_selective_run('flow1', ['Task4'], follow_subflows=False, run_subsequent=False)
        self._lists2sets(selective)

        assert selective['task_names'] == {'Task4'}
        # Yes, the third edge is correctly here as we expect all paths to Task4 to be present (even cyclic)
        assert selective['waiting_edges_subset'] == {'flow1': {0: {'Task2'}, 2: {'Task4'}, 3: {'Task2'}}}

    def test_compute_selective_run_multi_path(self):
        #
        # flow1:
        #         |      |
        #        T1     T2
        #         |      |
        #        T3     T3
        edge_table = {
            'flow1': [{'from': [], 'to': ['Task1'], 'condition': self.cond_true},
                      {'from': [], 'to': ['Task2'], 'condition': self.cond_true},
                      {'from': ['Task1'], 'to': ['Task3'], 'condition': self.cond_true},
                      {'from': ['Task2'], 'to': ['Task3'], 'condition': self.cond_true}]
        }
        self.init(edge_table)
        selective = compute_selective_run('flow1', ['Task3'], follow_subflows=False, run_subsequent=False)
        self._lists2sets(selective)

        assert selective['task_names'] == {'Task3'}
        assert selective['waiting_edges_subset'] == {'flow1': {0: {'Task1'}, 1: {'Task2'}, 2: {'Task3'}, 3: {'Task3'}}}

    def test_compute_selective_run_follow_subflows(self):
        #
        # flow1:
        #
        #      |
        #     T1
        #      |\
        #     T2 f2
        #      |
        #     T3
        #
        #
        # flow2:
        #
        #      |
        #     T4
        #      |
        #     T2
        #      |
        #     T5
        #
        edge_table = {
            'flow1': [{'from': [], 'to': ['Task1'], 'condition': self.cond_true},
                      {'from': ['Task1'], 'to': ['Task2'], 'condition': self.cond_true},
                      {'from': ['Task1'], 'to': ['flow2'], 'condition': self.cond_true},
                      {'from': ['Task2'], 'to': ['Task3'], 'condition': self.cond_true}],
            'flow2': [{'from': [], 'to': ['Task4'], 'condition': self.cond_true},
                      {'from': ['Task4'], 'to': ['Task2'], 'condition': self.cond_true},
                      {'from': ['Task2'], 'to': ['Task5'], 'condition': self.cond_true}]
        }
        self.init(edge_table)
        selective = compute_selective_run('flow1', ['Task2'], follow_subflows=True, run_subsequent=False)
        self._lists2sets(selective)

        assert selective['waiting_edges_subset'] == {'flow1': {0: {'Task1'}, 1: {'Task2'}, 2: {'flow2'}},
                                                     'flow2': {0: {'Task4'}, 1: {'Task2'}}}
        assert selective['task_names'] == {'Task2'}

    def test_compute_selective_run_follow_subflows_recursive(self):
        #
        # flow1:
        #
        #      |
        #     T1
        #      |\
        #     T2 f2
        #      |
        #     T3
        #
        #
        # flow2:
        #
        #      |
        #     T4
        #      |\
        #     T2 f3
        #      |
        #     T5
        #
        # flow3:
        #
        #      |
        #     T6
        #      |\
        #     T2 f1
        #      |
        #     T7
        #
        edge_table = {
            'flow1': [{'from': [], 'to': ['Task1'], 'condition': self.cond_true},
                      {'from': ['Task1'], 'to': ['Task2'], 'condition': self.cond_true},
                      {'from': ['Task1'], 'to': ['flow2'], 'condition': self.cond_true},
                      {'from': ['Task2'], 'to': ['Task3'], 'condition': self.cond_true}],
            'flow2': [{'from': [], 'to': ['Task4'], 'condition': self.cond_true},
                      {'from': ['Task4'], 'to': ['Task2'], 'condition': self.cond_true},
                      {'from': ['Task4'], 'to': ['flow3'], 'condition': self.cond_true},
                      {'from': ['Task2'], 'to': ['Task5'], 'condition': self.cond_true}],
            'flow3': [{'from': [], 'to': ['Task6'], 'condition': self.cond_true},
                      {'from': ['Task6'], 'to': ['Task2'], 'condition': self.cond_true},
                      {'from': ['Task6'], 'to': ['flow1'], 'condition': self.cond_true},
                      {'from': ['Task2'], 'to': ['Task7'], 'condition': self.cond_true}]
        }
        self.init(edge_table)
        selective = compute_selective_run('flow1', ['Task2'], follow_subflows=True, run_subsequent=False)
        self._lists2sets(selective)

        assert selective['waiting_edges_subset'] == {'flow2': {0: {'Task4'}, 1: {'Task2'}, 2: {'flow3'}},
                                                     'flow1': {0: {'Task1'}, 1: {'Task2'}, 2: {'flow2'}},
                                                     'flow3': {0: {'Task6'}, 1: {'Task2'}}}
        assert selective['task_names'] == {'Task2'}

    def test_compute_selective_run_subsequent(self):
        #
        # flow1:
        #
        #      |
        #     T1
        #      |
        #     T2
        #      |
        #     T3
        #
        #
        edge_table = {
            'flow1': [{'from': [], 'to': ['Task1'], 'condition': self.cond_true},
                      {'from': ['Task1'], 'to': ['Task2'], 'condition': self.cond_true},
                      {'from': ['Task2'], 'to': ['Task3'], 'condition': self.cond_true}],
        }
        self.init(edge_table)
        selective = compute_selective_run('flow1', ['Task2'], follow_subflows=False, run_subsequent=True)
        self._lists2sets(selective)

        assert selective['waiting_edges_subset'] == {'flow1': {0: {'Task1'}, 1: {'Task2'}, 2: {'Task3'}}}
        assert selective['task_names'] == {'Task2'}

    def test_compute_selective_run_follow_subflows_subsequent(self):
        #
        # flow1:
        #
        #      |
        #     T1
        #      |\
        #     T2 f2
        #      |
        #     T3
        #
        #
        # flow2:
        #
        #      |
        #     T4
        #      |
        #     T2
        #      |
        #     T5
        #
        edge_table = {
            'flow1': [{'from': [], 'to': ['Task1'], 'condition': self.cond_true},
                      {'from': ['Task1'], 'to': ['Task2'], 'condition': self.cond_true},
                      {'from': ['Task1'], 'to': ['flow2'], 'condition': self.cond_true},
                      {'from': ['Task2'], 'to': ['Task3'], 'condition': self.cond_true}],
            'flow2': [{'from': [], 'to': ['Task4'], 'condition': self.cond_true},
                      {'from': ['Task4'], 'to': ['Task2'], 'condition': self.cond_true},
                      {'from': ['Task2'], 'to': ['Task5'], 'condition': self.cond_true}]
        }
        self.init(edge_table)
        selective = compute_selective_run('flow1', ['Task2'], follow_subflows=True, run_subsequent=True)
        self._lists2sets(selective)

        assert selective['waiting_edges_subset'] == {'flow1': {0: {'Task1'}, 1: {'Task2'}, 2: {'flow2'}, 3: {'Task3'}},
                                                     'flow2': {0: {'Task4'}, 1: {'Task2'}, 2: {'Task5'}}}
        assert selective['task_names'] == {'Task2'}

    def test_compute_selective_run_bad_task_names(self):
        #
        # flow1:
        #
        #      |
        #     T1
        #      |
        #     T2
        #      |
        #     T3
        #
        #
        edge_table = {
            'flow1': [{'from': [], 'to': ['Task1'], 'condition': self.cond_true},
                      {'from': ['Task1'], 'to': ['Task2'], 'condition': self.cond_true},
                      {'from': ['Task2'], 'to': ['Task3'], 'condition': self.cond_true}],
        }
        self.init(edge_table)
        with pytest.raises(SelectiveNoPathError):
            compute_selective_run('flow1', ['TaskX'], follow_subflows=False, run_subsequent=True)
