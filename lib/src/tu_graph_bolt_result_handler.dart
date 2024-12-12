// Copyright (c) 2024- All tu_graph_bolt_dart_gdbc authors. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

part of '../tu_graph_bolt_dart_gdbc.dart';

class TuGraphResultHandler extends BoltResultHandler {
  @override
  ResultSet handle(List chunkValues) {
    var rs = BoltResultSet();
    var metas = chunkValues.removeAt(0);
    var bookmarkAndPlan = chunkValues.removeLast();

    var columnNames = metas['fields'] as ListValue;
    ValueMetaData meta = ValueMetaData();

    var rows = _handleDataSet(columnNames, meta, chunkValues.cast());
    rs.metas.addAll(meta.submetas);
    rs.rows.addAll(rows);
    return rs;
  }

  _handleDataSet(
      ListValue? cols, ValueMetaData meta, List<List<Value>>? chunkValues) {
    var rows = [];
    meta.submetas.addAll(
      cols
              ?.asList()
              .map(
                (e) => ValueMetaData()
                  ..name = e.asString()
                  ..type = GdbTypes.dataSet,
              )
              .toList() ??
          [],
    );

    rows = chunkValues
            ?.map((row) => List<dynamic>.filled(cols?.size ?? 0, null))
            .toList() ??
        [];

    for (var r = 0; r < (chunkValues?.length ?? 0); r++) {
      for (var c = 0; c < (cols?.size ?? 0); c++) {
        var value = chunkValues?[r][c];
        var submeta = meta.submetas[c];
        rows[r][c] = _handleValue(value!, submeta);
      }
    }
    return rows;
  }

  @override
  String get name => 'tu';
}

Map<GdbTypes, bool Function(dynamic)> typeGetter = {
  GdbTypes.none: (v) => (v is NullValue) || v == null,
  GdbTypes.prop: (v) => v is Map<String, Value?>,
  GdbTypes.node: (v) => v is NodeValue || v is Node,
  GdbTypes.relationship: (v) => v is RelationshipValue || v is Relationship,
  GdbTypes.path: (v) => v is PathValue || v is Path,

  GdbTypes.step: (v) => v is PathSegment,
  // GdbTypes.dataSet: (v) => v is ng.DataSet,

  GdbTypes.list: (v) => v is List,
  // GdbTypes.map: (v) => v is ng.Value && v.mVal != null,
  // GdbTypes.set: (v) => v is ng.Value && v.uVal != null,

  GdbTypes.bool: (v) => v is BooleanValue,
  GdbTypes.int: (v) => v is IntegerValue,
  GdbTypes.double: (v) => v is FloatValue,

  GdbTypes.string: (v) => v is StringValue,
  // GdbTypes.bytes: (v) => v is ng.Value && v.bVal != null,

  GdbTypes.date: (v) => v is DateValue,
  // GdbTypes.time: (v) => v is ng.Value && v.tVal != null,
  GdbTypes.dateTime: (v) => v is DateTimeValue,
  // GdbTypes.duration: (v) => v is ng.Value && v.duVal != null,

  // GdbTypes.geo: (v) => v is ng.Value && v.ggVal != null,
  // GdbTypes.line: (v) => v is ng.LineString,
  // GdbTypes.point: (v) => v is ng.Point,
  // GdbTypes.polygon: (v) => v is ng.Polygon,

  // must at last
  GdbTypes.unknown: (v) => true,
};

Map<GdbTypes, TypeHandler> typeHandler = {
  GdbTypes.none: (v, m) => null,
  GdbTypes.prop: (v, m) => _handleProp(v, m),
  GdbTypes.node: (v, m) => _handleNode(v, m),

  GdbTypes.relationship: (v, m) => _handleRelationship(v, m),
  GdbTypes.path: (v, m) => _handlePath(v, m),
  // //
  // GdbTypes.step: (v, m) => _handleStep(v, m),
  // GdbTypes.dataSet: (v, m) => handleDataSet(v, m),
  // //
  GdbTypes.list: (v, m) => _handleList(v, m),
  // GdbTypes.map: (v, m) => _handleMap(v.mVal, m),
  // GdbTypes.set: (v, m) => _handleSet(v.uVal, m),
  //
  GdbTypes.bool: (v, m) => (v as BooleanValue).asBoolean(false),
  GdbTypes.int: (v, m) => (v as IntegerValue).val,
  GdbTypes.double: (v, m) => (v as FloatValue).val,
  //
  GdbTypes.string: (v, m) => (v as StringValue).val,
  GdbTypes.bytes: (v, m) => throw UnimplementedError(), // TODO
  //
  GdbTypes.date: (v, m) => (v as DateValue).asLocalDate(),
  // GdbTypes.time: (v, m) => _handleTime(v.tVal, m),
  GdbTypes.dateTime: (v, m) => (v as DateTimeValue).asDateTime(),
  // GdbTypes.duration: (v, m) => _handleDuration(v.duVal, m),

  // GdbTypes.geo: (v, m) => _handleGeo(v, m),
  // GdbTypes.line: (v, m) => _handleLine(v, m),
  // GdbTypes.point: (v, m) => _handlePoint(v, m),
  // GdbTypes.polygon: (v, m) => _handlePolygon(v, m),

  // must at last
  GdbTypes.unknown: (v, m) => null,
};

_handleValue(
  dynamic v,
  ValueMetaData meta, {
  ValueMetaData? parent,
  List? parentVal,
}) {
  var type = typeGetter.entries.firstWhere((getter) => getter.value(v)).key;
  meta.type = type;
  var val = typeHandler[type]?.call(v, meta);
  parent?.addSubmeta(meta, parentVal, val);
  return val;
}

dynamic _nodeId(Node? node) {
  return node?.elementId ?? node?.id;
}

/// 关于创建节点的说明：[CREATE](https://github.com/TuGraph-family/tugraph-db/blob/c6e6ba16fe5ad9ec1c17f4badcd1bd3bf0316d93/docs/zh-CN/source/8.query/1.cypher.md#27create)，
/// **不支持创建多标签**，因此关于标签的解析，按单标签处理
_handleNode(dynamic v, ValueMetaData meta) {
  Node? n = v is Node ? v : v.asNode();

  var nodeData = [];

  // handle id
  ValueMetaData idMeta = ValueMetaData()..name = MetaKey.nodeId;
  var id = _nodeId(n);
  idMeta.type = id is int ? GdbTypes.int : GdbTypes.string;
  meta.addSubmeta(idMeta, nodeData, id);

  // 处理属性值
  var label = n?.labels.first;
  ValueMetaData tagMeta = ValueMetaData()..name = label;
  var tagVal = _handleValue(n?.computeMap<Value>(Values.value), tagMeta);
  meta.addSubmeta(tagMeta, nodeData, tagVal);

  return nodeData;
}

_handleRelationship(dynamic v, ValueMetaData meta) {
  InternalRelationship _v = v is InternalRelationship
      ? v
      : v.asRelationship() as InternalRelationship;
  var edgeData = [];

  ValueMetaData startNodeId = ValueMetaData()..name = MetaKey.startId;
  var startId = _v.startElementId ?? _v.start;
  startNodeId.type = startId is int ? GdbTypes.int : GdbTypes.string;
  meta.addSubmeta(startNodeId, edgeData, startId);

  ValueMetaData idMeta = ValueMetaData()
    ..name = MetaKey.relationshipId
    ..type = GdbTypes.int;
  meta.addSubmeta(idMeta, edgeData, _v.id);

  ValueMetaData endNodeId = ValueMetaData()..name = MetaKey.endId;
  var endId = _v.endElementId ?? _v.end;
  endNodeId.type = endId is int ? GdbTypes.int : GdbTypes.string;
  meta.addSubmeta(endNodeId, edgeData, endId);

  ValueMetaData edgeMeta = ValueMetaData()..name = _v.type;
  _handleValue(_v.computeMap(Values.value), edgeMeta,
      parent: meta, parentVal: edgeData);

  return edgeData;
}

_handleProp(
  Map<String, Value> props,
  ValueMetaData meta,
) {
  var propsVal = [];
  props.forEach((key, value) {
    var submeta = ValueMetaData()..name = key;
    var val = _handleValue(value, submeta);
    meta.addSubmeta(submeta, propsVal, val);
  });
  return propsVal;
}

_handlePath(dynamic v, ValueMetaData meta) {
  Path path = v is Path ? v : v.asPath();

  /// 列转行，数据解析时，需按行的逻辑进行解析
  var pathMeta = ValueMetaData()..type = GdbTypes.list;

  var pathData = <dynamic>[];

  ValueMetaData startNode = ValueMetaData()..name = '0';
  _handleValue(path.start, startNode, parent: pathMeta, parentVal: pathData);

  var pathList = path.toList();
  for (var i = 0; i < pathList.length; i++) {
    var step = pathList[i];

    ValueMetaData edgeMeta = ValueMetaData();
    var edge = step.relationship;

    _handleValue(edge, edgeMeta, parent: pathMeta, parentVal: pathData);

    ValueMetaData endNodeMeta = ValueMetaData()
      ..name = '${MetaKey.endNode}${i + 1}';
    _handleValue(step.end, endNodeMeta, parent: pathMeta, parentVal: pathData);
    edgeMeta.name = '${i + 1}';
    endNodeMeta.name = '${i + 1}';
  }

  var result = BoltResultSet()
    ..metas = pathMeta.submetas
    ..rows = [pathData];
  return result;
}

_handleList(List<dynamic> values, ValueMetaData meta) {
  var list = <dynamic>[];
  ValueMetaData valueMeta = meta.submetas.isEmpty
      ? (ValueMetaData()
        ..name = 'item'
        ..type = GdbTypes.unknown)
      : meta.submetas.first;
  for (var v in values) {
    var val = _handleValue(v, valueMeta, parent: meta);
    list.add(val);
  }
  return list;
}

typedef TypeHandler<T> = dynamic Function(T, ValueMetaData);
