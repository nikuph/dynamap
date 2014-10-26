function StringSet() {}
function NumberSet() {}
function BufferSet() {}

function ListType(type) {
  if (!(this instanceof ListType)) {
    return new ListType(type);
  }

  this.type = type;
}

function MapType(attributes) {
  if (!(this instanceof MapType)) {
    return new MapType(attributes);
  }

  this.attributes = attributes;
}

module.exports.StringSet = StringSet;
module.exports.NumberSet = NumberSet;
module.exports.BufferSet = BufferSet;
module.exports.List      = ListType;
module.exports.Map       = MapType;
