FYI:
PyArrow is suitable for processing smaller datasets, typically below 10 million records.
DuckDB is more suitable for our use case, as it provides better performance when handling large datasets.

Conclusion for the CIAMLHRC job:
In my code, the .arrow() function is a DuckDB method that converts query results into a PyArrow Table.
Since PyArrow stores data in memory (RAM), this operation causes the CIAMLHRC job to consume a large amount of memory.
As a result, the overall workflow performance becomes slower compared to last week.
