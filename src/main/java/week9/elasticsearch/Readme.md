Bài 3:
- Code python(main.py) để extract title từ các file data.json

- Sử dụng pivi (pyhton library tách từ tiếng Việt theo ngữ nghĩa) rồi ghi lại vào file data.json mới

- Tạo index title_suggest_ngoc dùng cho search suggestion

PUT title_suggest_ngoc
{
    "mappings": {
        "properties" : {
            "suggest_title" : {
                "type" : "completion"   
            }
        }
    }
}

- Đẩy dữ liệu lên ElasticSearch:

curl -s -H "Content-Type: application/json" -XPOST 10.140.0.8:9200/_bulk --data-binary @ngoc_data_1.json

- Search bằng query DSL:

{
    "suggest": {
        "title-suggest" : {
            "prefix" : "Thành",
            "completion" : {
                "field" : "suggest_title",
                "skip_duplicates": true,
                "size" : 10
            }
        }
    }
}

- Viết service bằng java (ElasticsService.java): getSuggestion(String input) để search suggestion.
