// Copyright (c) 2024- All tu_graph_bolt_dart_gdbc authors. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.
@Timeout(Duration(minutes: 2))
library;

import 'dart:convert';

import 'package:bolt_dart_gdbc/bolt_dart_gdbc.dart';
import 'package:tu_graph_bolt_dart_gdbc/tu_graph_bolt_dart_gdbc.dart';
import 'package:test/test.dart';

var url = 'localhost:8888';

void main() {
  group('A group of tests', () {
    late Connection conn;
    setUp(() async {
      TuGraphResultHandler();
      DriverManager.registerDriver(TuGraphDriver());
      conn = await DriverManager.getConnection(
          'gdbc.tu://$url?username=admin&password=73@TuGraph&db=default');
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

    // test('decr index', () async {
    //   var rs = await conn.executeQuery(r"MATCH (n:test)  RETURN count(n)");
    //   print(rs);
    // });

    test('decr insert', () async {
      var rs = await conn.executeQuery(
          r"""CREATE (n:fish{id: "0",name: "尼罗口孵非鲫",fish_img_url: "http://cdn.sea.fundiving.cn/fish_photo/1/cover.jpg_400.jpg",fish_imgs_url: "/fish_id/image/1.html",bio_kingdom_cn: "动物界",bio_kingdom_en: "Animalia",bio_phylum_cn: "脊索动物门",bio_phylum_en: "Chordata",bio_class_cn: "辐鳍鱼纲",bio_class_en: "Actinopterygii",bio_order_cn: "鲈形目",bio_order_en: "Perciformes",bio_family_cn: "丽鱼科",bio_family_en: "Cichlidae",bio_genus_cn: "口孵非鲫属(Oreochromi",bio_genus_en: "口 孵非鲫属(Oreochromis",bio_is_toxic: "否",bio_is_economy: "是",bio_is_food: "是",bio_is_view: "否",bio_other_name_url: "/fish_id/common_names/1.html",bio_introduction: "繁殖期的雄鱼的生殖乳突没有方格斑纹。  成熟雄鱼的颚不明显增大 (下颌 29-37% 的头长长度) 。  此鱼种的最显着特性是那在尾鳍的深度各处有规则的垂直斑纹.  背鳍的边缘灰色或黑色的。  在尾鳍 7-12 中的纵带."})""");
      print(rs);
    });

    test('decr q', () async {
      var rs = await conn.executeQuery(r"""MATCH (n:place)  RETURN count(n)""");
      var count = rs.rows[0][0];
      var skip = 0;

      while (skip < count) {
        print('count: $count, skip: $skip');
        rs = await conn
            .executeQuery("MATCH (n:fish)  RETURN n SKIP $skip LIMIT 100");
        skip += 100;
      }
      await conn.close();
    });
  });
}
