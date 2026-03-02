# Inventory Schema Validation Documentation

## Target Inventory Schema (MongoDB)

The inventory pipeline validates and maps incoming records to this logical schema:

- `inventory_id` (PK, string)
- `product_id` (FK -> products, string)
- `warehouse_id` (string)
- `available_stock` (int >= 0)
- `reserved_stock` (int >= 0)
- `damaged_stock` (int >= 0)
- `reorder_level` (int >= 0)
- `last_updated` (ISO-8601 timestamp)
- `batch_number` (string)

## Validation Rules Implemented

1. **Required columns present**: verifies all nine inventory columns are provided.
2. **Type checks**:
   - strict type checks on identifier/string fields
   - stock columns are cast to integers during cleansing
3. **Null/blank integrity checks**:
   - reject rows with blank IDs or missing `last_updated`
4. **Timestamp validation**:
   - parses `last_updated` into Spark timestamp; unparsable rows are quarantined
5. **Numeric guards**:
   - negative stock/reorder values are clamped to `0`
   - null numeric values are defaulted (`0` for stock, `25` for reorder level)
6. **De-duplication**:
   - duplicate `inventory_id` rows reduced to the most recent by `last_updated`

## MongoDB Representation

The transformed document structure is:

```json
{
  "_id": "INV-000001",
  "inventory_id": "INV-000001",
  "product_id": "PROD-00001",
  "warehouse_id": "WH-001",
  "stock": {
    "available": 125,
    "reserved": 4,
    "damaged": 1,
    "reorder_level": 40
  },
  "last_updated": "2026-01-01T04:00:00Z",
  "batch_number": "BATCH-0001",
  "pipeline_loaded_at": "2026-03-01T09:30:00Z"
}
```

## Suggested MongoDB Collection Validator

Use this JSON Schema validator on `inventory` collection:

```javascript
db.createCollection("inventory", {
  validator: {
    $jsonSchema: {
      bsonType: "object",
      required: ["_id", "inventory_id", "product_id", "warehouse_id", "stock", "last_updated"],
      properties: {
        _id: { bsonType: "string" },
        inventory_id: { bsonType: "string" },
        product_id: { bsonType: "string" },
        warehouse_id: { bsonType: "string" },
        stock: {
          bsonType: "object",
          required: ["available", "reserved", "damaged", "reorder_level"],
          properties: {
            available: { bsonType: "int", minimum: 0 },
            reserved: { bsonType: "int", minimum: 0 },
            damaged: { bsonType: "int", minimum: 0 },
            reorder_level: { bsonType: "int", minimum: 0 }
          }
        },
        last_updated: { bsonType: "string" },
        batch_number: { bsonType: "string" },
        pipeline_loaded_at: { bsonType: "date" }
      }
    }
  }
});
```
