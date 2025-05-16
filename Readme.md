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

    4. node position.js

    5. node user.js

    6. node user_group.js

    7. node course_category.js

    8. node course.js

    9. node chapter.js

    10. node lesson.js

    11  node news_category.js
    12. node news.js;
    13. node course_log.js;

    14. node course_lesson_log.js;
    15. node student_answer_log.js;
    16. node certificate.js
