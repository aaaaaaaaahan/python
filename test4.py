FYI,
PYARROW is suitable for small dataset below than 10 million raw data.
DUCKDB is suitable for us to process our dataset. And the performance will better than PYARROW when process with the high value dataset.

Conclusion for the CIAMLHRC:
In my code got .arrow() this is one of the PYARROW function.
PYARROW is using the RAM to process the data. So in this case CIAMLHRC is holding the RAM to cause the whole flow become slow performance compare with last week.
