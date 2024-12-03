// Copyright (c) 2024- All tu_graph_bolt_dart_gdbc authors. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

import 'dart:convert';

import 'package:bolt_dart_gdbc/bolt_dart_gdbc.dart';
import 'package:tu_graph_bolt_dart_gdbc/tu_graph_bolt_dart_gdbc.dart';
import 'package:test/test.dart';

void main() {
  group('A group of tests', () {
    late Connection conn;
    setUp(() async {
      TuGraphResultHandler();
      DriverManager.registerDriver(TuGraphDriver());
      conn = await DriverManager.getConnection(
          'gdbc.tu://139.9.187.207:7687?username=admin&password=73@TuGraph&db=default');
    });

    test('test String', () async {
      var rs = await conn.executeQuery(r"CALL db.vertexLabels() YIELD label");
      print(rs);
    });

    test('test node', () async {
      var rs = await conn.executeQuery(r"MATCH (n) RETURN n LIMIT 10");
      print(rs);
    });

    test('test relationship', () async {
      var rs =
          await conn.executeQuery(r"MATCH (n)-[r]-(m) RETURN n, r, m LIMIT 10");
      print(rs);
    });

    test('test path', () async {
      var rs =
          await conn.executeQuery(r"MATCH p = (n)-[r]-(m) RETURN p LIMIT 10");
      print(rs);
    });

    test('test list', () async {
      var rs = await conn.executeQuery(r"RETURN [3, 2]");
      print(rs);
    });

    test('test map', () async {
      var rs = await conn.executeQuery(r"CALL dbms.graph.getGraphSchema()");
      print(rs);
    });

    test('test map', () async {
      var rs = await conn.executeQuery(r"CALL dbms.meta.count()");
      print(rs);
    });

    test('test spaces', () async {
      var rs =
          await conn.executeQuery(r"CALL dbms.graph.listUserGraphs('admin')");
      print(rs);
    });

    test('test schema', () async {
      var rs = await conn.executeQuery(r"CALL db.vertexLabels()");
      print(rs);
    });

    test('decr edge info', () async {
      var rs = await conn.executeQuery(r"CALL db.getEdgeSchema('FRIEND_OF')");
      print(rs);
    });

    test('decr tag info', () async {
      var rs = await conn.executeQuery(r"CALL db.getVertexSchema('Person')");
      print(rs);
    });

    test('decr tag data c', () async {
      var rs = await conn.executeQuery(r"MATCH (n:Person) RETURN count(n)");
      print(rs);
    });

    test('decr tag data c', () async {
      var rs = await conn.executeQuery(r"MATCH (n:Position)  RETURN count(n)");
      print(rs);
    });

    test('decr tag query', () async {
      var rs = await conn.executeQuery(
          r"MATCH (n:Person) WHERE   n.name =~ 'Alice' RETURN n SKIP 0 LIMIT 30 ");
      print(rs);
    });

    test('decr tag query', () async {
      var rs = await conn.executeQuery(r"MATCH (n)-[r]-(m) RETURN n, r, m;");
      print(rs);
    });

    test('decr tag query', () async {
      var rs = await conn.executeQuery(
          r"CALL db.createVertexLabel('time_test', 'id', 'id', INT64, false, 'date', DATE, true, 'date_time', DATETIME, true)");
      print(rs);
    });
    test('decr time query2', () async {
      var rs = await conn.executeQuery(
          r"MATCH (src)-[r:KNOWS]->(dst)  RETURN src, r, dst SKIP 0 LIMIT 30");
      print(rs);
    });
    test('decr time query3', () async {
      var rs = await conn
          .executeQuery(r"MATCH (n:Person)  RETURN n SKIP 0 LIMIT 30");
      print(json.encode(rs));
    });
  });
}
