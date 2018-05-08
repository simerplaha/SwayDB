# Merge test cases

Merge tests should include tests for each key-value type merging with every other key-value type.

Each key-value contains key & deadline. All the following combination should be tested for key-values with deadline.

- `Key Deadline-HasTimeLeft-Greater` (new deadline > old deadline)
- `Key Deadline-HasTimeLeft-Lesser` (new deadline < old deadline)
- `Key Deadline-HasNoTimeLeft-Greater` (new deadline > old deadline)
- `Key Deadline-HasNoTimeLeft-Lesser` (new deadline < old deadline)
- `Key Deadline-Expired-Greater` (new deadline > old deadline)
- `Key Deadline-Expired-Lesser` (new deadline < old deadline)


# Test naming convention

_All tests following a naming convention. The naming convention makes it a little easier to group and visualize 
that all tests for all combinations of key-values are implemented._ 

- Remove(None) - Remove key-value with None deadline
- Remove(Some) - Remove key-value with any one of the above deadline

The same naming convention is followed for Update and Put

- Put(None, None) - Put key-value with None value and None deadline
- Put(Some, None) - Put key-value with None value and Some deadline
- Put(None, Some) - Put key-value with Some value and None deadline
- Put(Some, Some) - Put key-value with Some value and Some deadline

Update is similar to Put.