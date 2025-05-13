import mysql, { raw } from "mysql2/promise";
import { MongoClient, ObjectId } from "mongodb";
import dotenv from "dotenv";
import moment from "moment";

// Load biến môi trường từ .env
dotenv.config();

// Đọc cấu hình từ .env
const {
    MYSQL_HOST,
    MYSQL_USER,
    MYSQL_PASSWORD,
    MYSQL_DATABASE_COURSE,
    MONGO_DATABASE_COURSE,
    MONGO_URL,
    BATCH_SIZE,
    NEW_SITE_ID,
    OLD_SITE_ID,
    MYSQL_DATABASE_4T,
    MONGO_DATABASE_USER,
} = process.env;

// Kết nối MySQL
async function connectMySQL() {
    return mysql.createConnection({
        host: MYSQL_HOST,
        user: MYSQL_USER,
        password: MYSQL_PASSWORD,
        database: MYSQL_DATABASE_4T,
    });
}

async function deleteOldData(db, table) {
    await db.collection(table).deleteMany({
        siteId: +NEW_SITE_ID,
        oldId: { $ne: null }  // Ensures oldId is not null
    });
}

// Lấy dữ liệu theo từng batch
async function fetchBatch(sqlConnection, tableName, listExam, offset, limit) {

    if (listExam.length <= 0) return []

    const idsString = `(${listExam.map(id => `'${id}'`).join(',')})`;

    const query = `
        SELECT * FROM user_answers AS ua 
        JOIN users AS u ON ua.user_id = u.id
        WHERE ua.exam_id IN ${idsString}
        LIMIT ${parseInt(limit)} 
        OFFSET ${parseInt(offset)}
    `;

    const [rows] = await sqlConnection.execute(query);

    console.log(`🟢 Lấy ${rows.length} bản ghi từ ${tableName} (Offset: ${offset})`);
    return rows;
}

function createStudentAnswer(row, examSetRoomTest, userId) {

    const result = buildCandidateAnswers(examSetRoomTest, row.answer || []);

    const candidateExam = {
        candidateExamId: "", //examSetRoomTest
        userId: userId,
        examSetId: examSetRoomTest.examSetId,
        examSetRoomId: examSetRoomTest._id.toString(),
        roomTestId: examSetRoomTest.roomId,
        score: result?.score || 0,
        isPass: row.point > 5 ? true : false,
        igf: 0,
        timeTestStart: new Date(row.started_at),
        timeTestEnd: new Date(row.finished_at),
        enumTestStatus: 1,
        candidateAnswers: result?.ansArr || [],
        examActionCandidates: [
            // { action: 3, time: new Date("2025-05-06T10:49:13.970Z") }
        ],
        randomSeed: 0,
        comment: null,
        createdAt: new Date(row.created_at),
        timeTestStartClient: new Date(row.started_at),
        timeTestEndClient: new Date(row.finished_at),
        userName: null,
        guessAnswer: "",
        classId: "",
        courseId: "",
        lessonId: "",
        studentLessonId: "",
        typeCourse: 0,
        oldId: row.id,
        siteId: +NEW_SITE_ID
    };

    return candidateExam
}

async function findListExam(dbExam) {
    const rows = await
        dbExam.collection("examSetRoomTest")
            .find({
                siteId: +NEW_SITE_ID,
                oldId: { $ne: null }
            })
            .toArray();
    return rows;
}

async function findListUser(records, dbUser) {
    if (records.length === 0) return [];
    const ids = [...new Set(records.map(item => item.mobiedu_user_id))];
    const rows = await dbUser
        .collection("user")
        .find({ mobieduUserId: { $in: ids }, siteId: +NEW_SITE_ID })
        .toArray();
    return rows;
}

async function updateCompletePercent(userId, roomTestId, dbCourse, point = 0) {

    if (!roomTestId) return

    await dbCourse
        .collection("studentLesson")
        .updateOne(
            { userId, siteId: +NEW_SITE_ID, roomTestId },
            { $set: { completedPercent: point * 10 } }
        );
}

function buildCandidateAnswers(examSetRoomTest, listAnswer) {
    let score = 0

    const ansArr = examSetRoomTest.questionAndScores.map((item) => {
        let ansTrueExam = null;
        let matchedAns = null;

        const answers = item.question.listAnswer.map((ans, index) => {
            if (ans.isTrue) ansTrueExam = ans.select;

            const matched = listAnswer.find(
                (la) => la.oldId === ans.id && la.select === ans.select
            );

            if (matched) matchedAns = matched.select;

            return {
                order: index,
                textAnswer: ans.textAnswer,
                isTrue: !!matched,
                position: 0
            };
        });

        if (matchedAns === ansTrueExam && !!ansTrueExam) score++

        return {
            order: item.order,
            answers,
            score: item.score || 0,
            status: matchedAns === ansTrueExam && !!ansTrueExam
        };
    });

    return {
        score,
        ansArr
    }
}


async function migrateTable(sqlConnection, mongoDbExam, mongoDbCourse, mongoDbUser, tableName) {
    console.log(`🔄 Đang di chuyển bảng ${tableName}...`);

    await deleteOldData(mongoDbExam, "candidateExam")

    const listExam = await findListExam(mongoDbExam)
    const listExamIds = listExam.map(item => item.oldId)

    let offset = 0;
    // tạo lớp học 

    while (true) {
        const rows = await fetchBatch(sqlConnection, tableName, listExamIds, offset, BATCH_SIZE);
        const listUser = await findListUser(rows, mongoDbUser)

        const mappedStudentAnswer = []

        for (const row of rows) {

            const userInMongo = listUser.find(item => item.mobieduUserId == row.mobiedu_user_id)

            // bỏ qua nếu k phải là user của site hiện tại
            if (!userInMongo) continue

            const examSetRoomTest = listExam.find(item => item.oldId == row.exam_id)

            const userId = userInMongo?._id.toString()

            const newStudentAnswer = createStudentAnswer(row, examSetRoomTest, userId)
            await updateCompletePercent(userId, examSetRoomTest?.roomId, mongoDbCourse, row?.point)

            mappedStudentAnswer.push(newStudentAnswer);
        }
        // Kiểm tra nếu rows trống thì thoát khỏi vòng lặp
        if (!rows || rows.length === 0) break;

        await mongoDbExam.collection('candidateExam').insertMany(mappedStudentAnswer);

        offset += +BATCH_SIZE;
    }

}

const sqlConnection = await connectMySQL();

// Chạy quá trình di chuyển
async function migrate() {
    const mongoClient = new MongoClient(MONGO_URL);
    await mongoClient.connect();
    const mongoDbExam = mongoClient.db('db_moocs_exam');
    const mongoDbCourse = mongoClient.db(MONGO_DATABASE_COURSE);
    const mongoDbUser = mongoClient.db(MONGO_DATABASE_USER)

    try {
        await migrateTable(sqlConnection, mongoDbExam, mongoDbCourse, mongoDbUser, 'StudentAnswer');
    } catch (error) {
        console.error("❌ Lỗi khi di chuyển dữ liệu:", error);
    } finally {
        await sqlConnection.end();
        await mongoClient.close();
        console.log("🔚 Đã hoàn thành quá trình di chuyển.");
    }
}

migrate();