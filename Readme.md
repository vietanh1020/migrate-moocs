# Chuân bị: Tạo site + tạo adminSite trước

B1: Tạo site Mới và tạo tài khoản Admin - Lấy ID site mới

B2: Sửa .env

- Sửa tất cả env của MYSQL

* Sửa OLD_SITE_ID: là ID của site ở Hệ thống cũ
* NEW_SITE_ID: là ID site mới lấy ở B1

# B3: Chạy lần lượt các script để Migrate

# Sơ đồ tổ chức

    1. node manager_level_1.js
    2. node manager_level_2.js
    3. node manager_level_3.js

    4. node user.js

    5. node user_group.js

    6. node course_category.js

    7. node course.js

    8. node chapter.js

    9. node lesson.js

    7. node course_log.js

    10  node news_category.js
    11. node news.js;
