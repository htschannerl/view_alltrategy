# Views ERP Sapiens with Oracle

The objetive was create one View to import the values from ERP system to the budget system.

## Chalange

Create one view with the main information from the accounting and the source of the registry, which can be Invoice, Stock, Financial or any module of the ERP system.

Also, the ERP system possible for each module to have the values divided into many cost centers.

## Solution

Split in various views, each for one module considering all possibilities.

On top of that, was created the following views:
- USU_VALLSTCPA.sql
- USU_VALLSTEST.sql
- USU_VALLSTNFE.sql
- USU_VALLSTNFS.sql
- USU_VALLSTREC.sql
- USU_VALLSTRES.sql
- USU_VALLSTTES.sql

And finally the view **USU_VALLST.sql** with the union of all above.

## Dependence

For every view to work properly, need to create the function **histpadrao.sql**.
This function is for transforming the default history to the history with the comment from the record.

## Future problem

With the increase of records in the database, it can be a problem to use one view to query information from other views.