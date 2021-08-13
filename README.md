rdmo-merge
==========

This script can be used to merge the content of two [RDMO](https://github.com/rdmorganiser/rdmo) instances. More precice, It allows the ingest of a *secondary* RDMO instance into a *primary* instance, while keeping all foreign key relations intact and adding content elements which do not exist in the primary instance, yet.

Setup
-----

The `merge.py` scripts needs Python3 without additional packages. It works on fixtures created from the instances. Both instance need to be updated to the latest version of RDMO. In particular, all database migrations need to be performed.

Usage
-----

1. Please create additonal backups of all relevant data, this is a rather complicated process. Things can go wrong.

2. Create fixture dumps for both instances. Remember the instances need to be fully upgraded to the latest version.

    ```bash
    (env) ~/rdmo-primary$ ./manage.py dumpdata > primary.json
    (env) ~/rdmo-secondary$ ./manage.py dumpdata > secondary.json
    ```

3. Run `merge.py` with the two dumps:

    ```bash
    (env) ~/rdmo-merge$ ./merge.py primary.json secondary.json output.json
    ```
    
    Warnings are created when an element of secodary is found in primary, but has a different value. This can happen if the element was changed
    whithout changing the URI.

4. Load the created fixture into the primary instance:

    ```bash
    (env) ~/rdmo-primary$ ./manage.py loaddata output.json
    ```
