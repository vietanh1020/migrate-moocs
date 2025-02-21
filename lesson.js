import dotenv from "dotenv";
import { MongoClient } from "mongodb";
import mysql from "mysql2/promise";

// Load biến môi trường từ .env
dotenv.config();

// Đọc cấu hình từ .env
const {
    MYSQL_HOST,
    MYSQL_USER,
    MYSQL_PASSWORD,
    MYSQL_DATABASE_COURSE,
    MONGO_URL,
    MONGO_DATABASE_COURSE,
    BATCH_SIZE,
    OLD_SITE_ID,
} = process.env;

// Kết nối MySQL
async function connectMySQL() {
    return mysql.createConnection({
        host: MYSQL_HOST,
        user: MYSQL_USER,
        password: MYSQL_PASSWORD,
        database: MYSQL_DATABASE_COURSE,
    });
}

// Lấy dữ liệu theo từng batch
async function fetchBatch(sqlConnection, tableName, offset, limit) {
    const query = `
    SELECT * FROM ${tableName} 
    WHERE IdSite = ${parseInt(OLD_SITE_ID)} 
    AND IdParent IS NOT NULL 
    LIMIT ${parseInt(limit)} 
    OFFSET ${parseInt(offset)}
    `;

    const [rows] = await sqlConnection.execute(query);

    console.log(`🟢 Lấy ${rows.length} bản ghi từ ${tableName} (Offset: ${offset})`);
    return rows;
}

async function findChapter(records, db) {
    if (records.length === 0) return [];
    const ids = [...new Set(records.map(item => item.oldId))];
    const rows = await db.collection("chapter").find({ oldId: { $in: ids } }).toArray();
    return rows;
}


async function getType(row) {

    if (row.VideoFileType == "Video") return 0;
    if (row.VideoFileType == "Youtube") return 1
    if (row.Name == "File ghi hình trực tuyến") return 3;
    if (row.Name == "File text") return 4;
    if (row.Name == "Test" && row.ExamType == 'room') return 5;


    // [Description("Dạng Video")] VIDEO = 0,
    // [Description("Dạng Youtube")] YOUTUBE = 1,
    // [Description("Dạng Audio")] AUDIO = 2,
    // [Description("Dạng bài giảng tương tác")] INTERACTIVE_LESSON = 3,
    // [Description("Dạng tài liệu")] FILE = 4,
    // [Description("Dạng Bài tập/ Kiểm tra")] TEST = 5,
    // [Description("Dạng kiểm tra cuối khoá")] FINAL_TEST = 6,
}


async function getTypeEnd(row) {
    if (row.VideoFileType == "Video" || row.VideoFileType == "Youtube") return 0;
    if (row.Name == "Bài kiểm tra") return 1
    if (row.Name == "Test" && row.ExamType == 'room') return 1;

    return 2
}


async function migrateTable(sqlConnection, mongoDb, tableName) {
    const chapters = await findChapter(rows, mongoDbCourse);
    console.log(`🔄 Đang di chuyển bảng ${tableName}...`);


    let offset = 0;
    while (true) {
        const rows = await fetchBatch(sqlConnection, tableName, offset, BATCH_SIZE);
        const mappedNews = rows.map((row) => {
            const chapter = chapters.find(item => row.ParentId == item.oldId);
            return {
                chapterId: chapter?._id,
                courseId: chapter?.courseId,
                name: row.Name,
                lessonType: getType(row),
                urlLessonType: row.VideoFileUrl,
                uriVideo: row.VideoFileUrl,
                urlFileAttended: JSON.stringify(row.FileUrls),
                fileAttendedName: JSON.stringify(row.FileUrls),
                tag: [],
                status: 1,
                urlAvatar: "",
                description: row.Description || row.Content,
                lessonFinishStatus: getTypeEnd(row),
                percentFinish: 100,
                questions: [], // câu hỏi
                score: 0, // tổng điểm
                lessonStatus: 1,
                order: row.DisplayOrder,
                testId: null,
                duration: 0,
                thumbnailUrl: row.ThumbnailFileUrl,
                numberQuestionPass: 0, //làm đúng bao nhêu câu
                markPassExam: 0, //điểm  pass bài
                createAt: row.CreatedDate,
                updateAt: row.ModifiedDate,

                oldId: row.oldId,
                oldIdExam: row.IdExam,
            }
        });

        // Kiểm tra nếu rows trống thì thoát khỏi vòng lặp
        if (!rows || rows.length === 0) break;

        await mongoDb.collection('lesson').insertMany(mappedNews);

        offset += BATCH_SIZE;
    }

    console.log(`🏁 Hoàn tất di chuyển bảng ${tableName}!`);
}

const sqlConnection = await connectMySQL();

// Chạy quá trình di chuyển
async function migrate() {
    const mongoClient = new MongoClient(MONGO_URL);
    await mongoClient.connect();
    const mongoDb = mongoClient.db(MONGO_DATABASE_COURSE);

    try {
        await migrateTable(sqlConnection, mongoDb, 'CourseLesson');
    } catch (error) {
        console.error("❌ Lỗi khi di chuyển dữ liệu:", error);
    } finally {
        await sqlConnection.end();
        await mongoClient.close();
        console.log("🔚 Đã hoàn thành quá trình di chuyển.");
    }
}

migrate();