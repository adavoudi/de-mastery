isFlinkPojo only checks the class is public, so non-POJO BadEvent (private fields, no no-arg ctor) wrongly passes.
