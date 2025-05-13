columns = [
  {
    "name": "show_id",
    "type": "VARCHAR"
  },
  {
    "name": "type",
    "type": "VARCHAR"
  },
    {
    "name": "title",
    "type": "VARCHAR"
  },
    {
    "name": "director",
    "type": "VARCHAR"
  }
]

query = {
  "select": [
    {
      "property": {
        "property": "show_id"
      }
    },
        {
      "property": {
        "property": "type"
      }
    },
        {
      "property": {
        "property": "title"
      }
    },
        {
      "property": {
        "property": "director"
      }
    },
  ],
  "where": {
    "equalA": {
      "property": "country"
    },
    "equalB": {
      "stringValue": "United States"
    }
  }
}