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

"show_id,type,title,director"

query = {
  "where": {
    "equalA": {
      "property": "country"
    },
    "equalB": {
      "stringValue": "United States"
    }
  }
}