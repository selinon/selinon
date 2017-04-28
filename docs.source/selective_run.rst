.. _selective-run:

Selective task run
==================

 * there has to be direct path to desired task, not via failure edges
 * if there are multiple paths to desired task/tasks all of them are run
 * if there is a direct or indirect cyclic edge to a task, this cyclic edge is included
 *