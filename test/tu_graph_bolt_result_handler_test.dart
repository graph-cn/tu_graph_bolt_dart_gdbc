// Copyright (c) 2024- All tu_graph_bolt_dart_gdbc authors. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

import 'package:bolt_dart_gdbc/bolt_dart_gdbc.dart';
import 'package:test/test.dart';
import 'package:tu_graph_bolt_dart_gdbc/tu_graph_bolt_dart_gdbc.dart';

main() {
  group('A group of tests', () {
    TuGraphResultHandler? handler;
    setUp(() {
      handler = TuGraphResultHandler();
    });

    test('Test String', () {
      List data = <dynamic>[
        {
          "fields": [StringValue('label')],
        },
        [StringValue("Person")],
        {},
      ];
      ResultSet brs = handler!.handle(data);
      print(brs);
      expectAll([0], GdbTypes.string, 'label', 'Person', brs);
    });
  });
}

void expectAll(
  List<int> col,
  GdbTypes? type,
  String? name,
  dynamic val,
  ResultSet result,
) {
  var meta = result.meta(col);
  if (type != null) {
    expect(meta?.type, type);
  }
  if (name != null) expect(meta?.name, name);
  var v = result.value(col);
  if (val != null) expect(v, val);
}
