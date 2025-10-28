// Copyright (c) 2024- All tu_graph_bolt_dart_gdbc authors. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

part of '../tu_graph_bolt_dart_gdbc.dart';

class TuGraphDriver extends BoltDriver {
  @override
  String get name => 'tu';

  @override
  Future<Connection> connect(
    String url, {
    Map<String, dynamic>? properties,
    Function()? onClose,
  }) async {
    var conn = await super.connect(
      url,
      properties: properties,
      onClose: onClose,
    );
    var rs = await conn.executeQuery("CALL dbms.system.info()");
    var version = getVersion(rs);
    conn.version = version;
    return conn;
  }

  String? getVersion(ResultSet rs) {
    var infoNameIdx = rs.metas.indexWhere((e) => e.name == 'name');
    var infoValueIdx = rs.metas.indexWhere((e) => e.name == 'value');
    if (infoNameIdx == -1 || infoValueIdx == -1) {
      return null;
    }
    for (var i = 0; i < rs.rows.length; i++) {
      if (rs.rows[i][infoNameIdx] == 'lgraph_version') {
        return rs.rows[i][infoValueIdx];
      }
    }
    return null;
  }
}
