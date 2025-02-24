import dotenv from "dotenv";
import moment from "moment";
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
    MYSQL_DATABASE_4T,
    MONGO_URL,
    MONGO_DATABASE_COURSE,
    BATCH_SIZE,
    NEW_SITE_ID,
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

async function connectDB_4T() {
    return mysql.createConnection({
        host: MYSQL_HOST,
        user: MYSQL_USER,
        password: MYSQL_PASSWORD,
        database: MYSQL_DATABASE_4T,
    });
}

async function findRoomTest(records) {
    if (records.length == 0) return [];
    const ids = [...new Set(records.map(item => item.IdExam))].filter(item => !!item);
    const formattedIds = ids.map(id => `'${id}'`).join(",");

    const query = `SELECT * FROM rooms WHERE id IN (${formattedIds})`;
    const [rows] = await sqlConnection4t.execute(query);
    return rows;
}

async function findQuestionExam(roomId) {
    if (!roomId) return;

    if (records.length == 0) return [];
    const query = `SELECT * FROM exams WHERE room_id='${roomId}'`;
    const [rows] = await sqlConnection4t.execute(query);
    const questions = rows.map(item => {
        return {
            questionText: item.question.content,
            listAnswer: item.answers.map((ans, index) => {
                return {
                    texAnswer: ans.content,
                    isTrue: ans.id === item.correct,
                    position: index,
                }
            }),
            score: item.point
        }
    })

    return questions;
}

// Lấy dữ liệu theo từng batch
async function fetchBatch(sqlConnection, tableName, offset, limit, courseOldId) {
    const query = `
    SELECT * FROM ${tableName} 
    WHERE IDCourse IN (${courseOldId.map(id => parseInt(id)).join(",")})
    AND IdParent IS NOT NULL 
    LIMIT ${parseInt(limit)} 
    OFFSET ${parseInt(offset)}
    `;

    const [rows] = await sqlConnection.execute(query);

    console.log(`🟢 Lấy ${rows.length} bản ghi từ ${tableName} (Offset: ${offset})`);
    return rows;
}

async function findChapter(records, db) {
    const ids = [...new Set(records.map(item => item._id.toString()))];
    const rows = await db.collection("chapter").find({ courseId: { $in: ids } }).toArray();
    return rows;
}


function getType(row) {

    if (row.VideoFileType == "Video") return 0;
    if (row.VideoFileType == "Youtube") return 1;
    if (row.Name == "File ghi hình trực tuyến") return 3;
    if (row.Name == "File text") return 4;
    if (row.Name == "Test" && row.ExamType == 'room') return 5;

    return 0;

    // [Description("Dạng Video")] VIDEO = 0,
    // [Description("Dạng Youtube")] YOUTUBE = 1,
    // [Description("Dạng Audio")] AUDIO = 2,
    // [Description("Dạng bài giảng tương tác")] INTERACTIVE_LESSON = 3,
    // [Description("Dạng tài liệu")] FILE = 4,
    // [Description("Dạng Bài tập/ Kiểm tra")] TEST = 5,
    // [Description("Dạng kiểm tra cuối khoá")] FINAL_TEST = 6,
}


function getTypeEnd(row) {
    if (row.VideoFileType == "Video" || row.VideoFileType == "Youtube") return 0;
    if (row.Name == "Bài kiểm tra") return 1
    if (row.Name == "Test" && row.ExamType == 'room') return 1;

    return 2
}

async function findCourseBySite(db) {
    const rows = await db.collection("course").find({ siteId: +NEW_SITE_ID }).toArray();
    return rows;
}



async function migrateTable(sqlConnection, mongoDb, tableName) {
    console.log(`🔄 Đang di chuyển bảng ${tableName}...`);


    const courseInMongo = await findCourseBySite(mongoDb)

    const courseOldId = courseInMongo.map(item => item.oldId)

    const chapters = await findChapter(courseInMongo, mongoDb);
    let offset = 0;
    while (true) {
        const rows = await fetchBatch(sqlConnection, tableName, offset, BATCH_SIZE, courseOldId);

        const RoomTestBatch = await findRoomTest(rows)

        const mappedLesson = [];




        for (const row of rows) {
            const roomTest = RoomTestBatch.find(item => row.IdExam == item.id);

            const chapter = chapters.find(item => row.IDParent == item.oldId);

            const questionsExam = await findQuestionExam(roomTest?.exam_id) || [];

            mappedLesson.push({
                chapterId: chapter?._id.toString(),
                courseId: chapter?.courseId.toString(),
                name: row.Name,
                lessonType: getType(row),
                urlLessonType: row.VideoFileUrl,
                uriVideo: row.VideoFileUrl,
                urlFileAttended: row.FileUrls ? JSON.stringify(row.FileUrls) : null,
                fileAttendedName: row.FileUrls ? JSON.stringify(row.FileUrls) : null,
                tag: [],
                status: 1,
                urlAvatar: "",
                description: row.Description || row.Content,
                lessonFinishStatus: getTypeEnd(row),
                percentFinish: 100,
                questions: questionsExam, // câu hỏi
                score: questionsExam.reduce((sum, item) => sum + item.score, 0), // tổng điểm
                lessonStatus: 1,
                order: +row.DisplayOrder,
                testId: null,
                duration: 0,
                thumbnailUrl: row.ThumbnailFileUrl,
                numberQuestionPass: 0, //làm đúng bao nhiêu câu
                markPassExam: +roomTest?.pass_point || 0, // điểm pass bài
                createAt: moment(row.CreatedDate).unix(),
                updateAt: moment(row.ModifiedDate).unix(),
                oldId: row.oldId,
                oldIdExam: row.IdExam,
            });
        }


        // Kiểm tra nếu rows trống thì thoát khỏi vòng lặp
        if (!rows || rows.length === 0) break;

        await mongoDb.collection('lesson').insertMany(mappedLesson);

        offset += +BATCH_SIZE;
    }

    console.log(`🏁 Hoàn tất di chuyển bảng ${tableName}!`);
}

const sqlConnection = await connectMySQL();
const sqlConnection4t = await connectDB_4T()

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