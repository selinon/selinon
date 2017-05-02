.. _migration:

Migration from raw Celery to Selinon
------------------------------------

Supposing you are already using Celery and you want to migrate to Selinon, this section will give you a very high overview on what needs to be done and what are possible pitfalls when migrating. Also check :ref:`faq` for some notes on why you should and why you shouldn't migrate to Selinon.

* Make sure you use Celery of version 4. Older versions of Celery are not supported by Selinon.
* Make sure your result backend is some persistent storage (such as Redis or PostgreSQL). If you use `rpc`, Selinon will not work properly.
* If you use Celery's tasks, the transition will be more smooth (just change base class to :class:`SelinonTask <selinon.selinonTask.SelinonTask>`). If you are using Celery functions you need to encapsulate them to the mentioned class.
* If you are using Celery primitives, remove them. Instead define Selinon YAML configuration.
* If you are using features like ``self.retry()`` or you raise Celery specific exceptions, rewrite semantics to YAML configuration and remove these Celery-specific pieces. Also note that changing arguments for a task by ``self.retry()`` is not supported by Selinon.
* Pass application context and configuration files to Selinon :class:`Config <selinon.config.Config>`


* **Feel free to extend this list...**
