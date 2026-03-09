# フロー・CDF・AUTO CDC INTO

## フロー

[Qiita｜Lakeflow SDP入門者の鬼門：ストリーミングテーブルとマテリアライズドビューを完全理解する](https://qiita.com/taka_yayoi/items/40073e5a9a60d7a08384) を参考にフローとAUTO CDC INTOを解説する。

- マテリアライズドビューとストリーミングテーブルの定義方法の違いから捉える「フロー」の概念
  - マテリアライズドビュー：「テーブル定義」と「データの流し込み方」が一体
  - ストリーミングテーブル：「テーブル定義」と「データの流し込み方」が分離
  - [Qiita｜Lakeflow SDP入門者の鬼門：ストリーミングテーブルとマテリアライズドビューを完全理解する #フローとは何か：STとMVの定義方法の違い](https://qiita.com/taka_yayoi/items/40073e5a9a60d7a08384#4-%E3%83%95%E3%83%AD%E3%83%BC%E3%81%A8%E3%81%AF%E4%BD%95%E3%81%8Bst%E3%81%A8mv%E3%81%AE%E5%AE%9A%E7%BE%A9%E6%96%B9%E6%B3%95%E3%81%AE%E9%81%95%E3%81%84)
- このデータの流し込み方が「フロー」

## CDF

CDF(変更データフィード) = 以下のような「テーブルの変更履歴ログ」と考えると理解しやすいです。

| userId | name   | \_change_type | \_ingest_timestamp |
| ------ | ------ | ------------- | ------------------ |
| 1      | Alice  | INSERT        | 2026-01-08         |
| 1      | Alicia | UPDATE        | 2026-02-08         |
| 2      | Bob    | INSERT        | 2026-03-08         |
| 1      | —      | DELETE        | 2026-03-08         |

> 参考までに）データに対する変更（＝CDC）をリストにしたものを、一般的に *CDC フィード* といい、
> 変更データフィード（CDF）はDeltaテーブル独自のCDCフィードのことを指す。

参考までに）[Azure Databricks｜CDF確認SQL](https://learn.microsoft.com/ja-jp/azure/databricks/delta/delta-change-data-feed#sql)

```sql
-- version as ints or longs e.g. changes from version 0 to 10
SELECT * FROM table_changes('tableName', 0, 10)
```

## AUTO CDC INTO

> AUTO CDCは以下を自動的に行います：
>
> 1. KEYS (id) で同じidのレコードを特定
> 2. SEQUENCE BY ts でイベントの順序を判定(遅延データにも対応)
> 3. INSERT → 新規行を追加
> 4. UPDATE → 既存行を更新
> 5. DELETE → 既存行を削除

## SCD設定

参考）[Databricks｜変更データフィードの処理: 最新データのみを保持するか、データの履歴バージョンを保持するか](https://docs.databricks.com/aws/ja/ldp/what-is-change-data-capture#%E5%A4%89%E6%9B%B4%E3%83%87%E3%83%BC%E3%82%BF%E3%83%95%E3%82%A3%E3%83%BC%E3%83%89%E3%81%AE%E5%87%A6%E7%90%86-%E6%9C%80%E6%96%B0%E3%83%87%E3%83%BC%E3%82%BF%E3%81%AE%E3%81%BF%E3%82%92%E4%BF%9D%E6%8C%81%E3%81%99%E3%82%8B%E3%81%8B%E3%83%87%E3%83%BC%E3%82%BF%E3%81%AE%E5%B1%A5%E6%AD%B4%E3%83%90%E3%83%BC%E3%82%B8%E3%83%A7%E3%83%B3%E3%82%92%E4%BF%9D%E6%8C%81%E3%81%99%E3%82%8B%E3%81%8B)

> 参考）[Databricks｜履歴追跡の有効化 (SCD タイプ 2)](https://docs.databricks.com/aws/ja/ingestion/lakeflow-connect/scd)
>
> 履歴追跡設定は、 slowly changing dimensions (SCD) 設定とも呼ばれ、時間の経過に伴うデータの変更を処理する方法を決定します。

- SCDタイプ1：既存のデータを上書きし、変更履歴を保存しない
  - エラーの修正や顧客の E メール アドレスなどの重要でないフィールドの更新など、変更履歴が重要でない場合によく使用されます。
- SCDタイプ2：データの変更履歴を保存
  - 分析の目的で時間の経過に伴う顧客住所の変更を追跡するなど、データの進化を追跡することが重要な場合に役立ちます。

## まとめ

これらを踏まえて、

```
CREATE FLOW scd_type1_bronze_to_silver
AS
AUTO CDC INTO sdp_example_silver_audit
FROM table_name
KEYS (userId)
SEQUENCE BY _ingest_timestamp
STORED AS SCD TYPE 1;
```

を実行すると

```
CDF入力
userId=1 Alice   INSERT
userId=1 Alicia  UPDATE
userId=2 Bob     INSERT
userId=1         DELETE

        ↓ AUTO CDC INTO

最終テーブル
userId=2 Bob
```

### つまり フロー・CDF・AUTO CDC INTOとは？

- ストリーミングテーブルでは、「データの流し込み方」を定義する
  - ↑これがフロー。
- 「流し込み方」（手段）として 「INSERT INTO」と「AUTO CDC INTO」 を選択できる
- 「流し込み」には、CDF（Databricksにおけるテーブルの変更履歴ログ）を使う

### 注意

AUTO CDCは重複排除機能でなく、
AUTO CDCで変更イベントを適用した結果、重複が1件になる。
