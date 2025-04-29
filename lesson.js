import dotenv from "dotenv";
import { MongoClient, ObjectId } from "mongodb";
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
    if (ids.length == 0) return [];
    const formattedIds = ids.map(id => `'${id}'`).join(",");

    const query = `SELECT * FROM rooms WHERE id IN (${formattedIds})`;
    const [rows] = await sqlConnection4t.execute(query);
    return rows;
}


async function findAllQues(sqlConnection4t, exam_id) {
    if (!exam_id) return []
    const query = `SELECT * FROM questions WHERE exam_id='${exam_id}'`;
    const [rows] = await sqlConnection4t.execute(query);
    return rows
}

function getTypeQuestion(type) {
    if (type === "TRUE_FALSE") return 1
    if (type === "ONE") return 2
    if (type === "MULTI") return 3
    if (type === "GROUP") return 4
    if (type === "CONNECT") return 5
    if (type === "FILL_WORD") return 6
    if (type === "SORT") return 7
    if (type === "ESSAY") return 8

    if (type === "DRAW") return 10
}

async function findQuestionExam(roomTest, mongoDbCourse, mongoDbExam) {

    const roomId = roomTest?.id;

    if (!roomId) return "";

    const query = `SELECT * FROM exams WHERE room_id='${roomId}'`;
    const [rows] = await sqlConnection4t.execute(query);

    if (rows.length == 0) return []

    const examInMysql = rows?.[0]

    const allQues = await findAllQues(sqlConnection4t, examInMysql.id)

    return await createNewExam(mongoDbCourse, mongoDbExam, roomTest, examInMysql, allQues)
}

async function createNewExam(mongoDbCourse, mongoDbExam, roomInMysql, examInMysql, originQues) {

    const allQues = originQues.sort((a, b) => a.index - b.index)
    if (allQues.length == 0) return null

    const groupIdMap = {};

    allQues.forEach((element, index) => {
        if (element.group_id != null) {
            if (!groupIdMap[element.group_id]) {
                groupIdMap[element.group_id] = {
                    objectId: new ObjectId(),
                    index: index
                };
            } else {
                groupIdMap[element.group_id].index = index; // hoặc giữ index đầu tiên tùy ý đồ
            }
        }
    });

    const questions = allQues.map((item) => {
        return {
            order: item.index,
            question: {
                _id: item?.parent_id ? new ObjectId() : groupIdMap[item.group_id]?.objectId,
                siteId: +NEW_SITE_ID,
                questionText: item.question.content,
                questionMedia: "",
                urlImageQuestion: "",
                urlAttactedFile: "",
                explain: "",
                questionCatalogId: null,
                questionType: getTypeQuestion(item.type),
                levelQuestionId: "",
                listAnswer: item.answers.map((ans, index) => {
                    return {
                        textAnswer: ans.content,
                        isTrue: ans.id === item.correct,
                        position: index,
                    }
                }),
                scoringInstructions: "",
                isActive: true,
                isShuffleAnswer: false,
                groupQuestionId: item.parent_id ? groupIdMap[item.parent_id].objectId.toString() : "",
                createdAt: item.created_at,
                updateAt: item.updated_at || 0,
                createBy: "",
                updateBy: "",
            },
            parent_id: item.parent_id,
            group: item.parent_id ? groupIdMap[item.parent_id].index : item.group_id ? groupIdMap[item.group_id].index : -1,
            score: item.poin == 0 ? 0 : 1,
            status: true,
        }
    })

    const newExamSet = {
        _id: new ObjectId(),
        examSetName: examInMysql.name,
        siteId: +NEW_SITE_ID,
        typeExam: 1,
        examSyllabusId: "",
        status: 2,
        firstWord: "Câu",
        scoreEachQuestion: 0,
        isShuffleQuestion: false,
        isShuffleAnswer: false,
        urlFileAttended: "",
        scoreScale: questions.reduce((sum, item) => sum + item.score, 0),
        questionAndScores: questions,
        jsonRandomRequest: "",
        isTest: false,
        createdAt: examInMysql.created_at,
        updatedAt: examInMysql.updated_at,
        createBy: "",
        oldId: examInMysql.id
    };

    const newRoomTest = {
        _id: new ObjectId(),
        roomTestName: roomInMysql.name,
        examName: examInMysql.name,
        isShowCodeExam: false,
        enumTypeRoom: 2,
        timeTestOpen: roomInMysql?.start_time || new Date(),
        timeTestFinish: new Date('2035-01-01'),
        examDuration: roomInMysql.duration,
        enumMonitorExam: 1,
        allowAttempts: 1,
        settingResultExams: [1],
        timeViewExams: [1],
        toltalUser: 0,
        totalUserTest: 0,
        passingPercentage: 50,
        status: 1,
        proctors: [],
        examiners: [],
        gradingTime: new Date(),
        urlGradingOutline: null,
        guessQuestion: null,
        groupCandidate: [],
        candidate: [],
        candidateIGF: null,
        enumGetExam: 1,
        totalExamRandom: 1,
        enumShuffles: [1, 2],
        examSets: [newExamSet._id.toString()],
        mCertificateId: null,
        siteId: +NEW_SITE_ID,
        createdAt: roomInMysql.created_at,
        updatedAt: roomInMysql.updated_at,
        createBy: "67ce5d516c187b3adcc8844a",
        updateBy: null,
        isHidden: false,
        oldId: roomInMysql.id
    };

    const examSetRoomTest = {
        roomId: newRoomTest._id.toString(),
        examName: examInMysql.name,
        examSetId: newExamSet._id.toString(),
        examSetName: newExamSet.examSetName,
        examNumberID: `THR${Math.floor(100 + Math.random() * 900)}`,
        examDuration: newRoomTest.examDuration,
        isShuffleQuestion: newExamSet.isShuffleQuestion,
        isShuffleAnswer: newExamSet.isShuffleAnswer,
        scoreScale: newExamSet.scoreScale,
        scoreEachQuestion: newExamSet.scoreEachQuestion,
        urlFileAttended: "",
        firstWord: newExamSet.firstWord,
        questionAndScores: newExamSet.questionAndScores.map((q, index) => ({
            order: index,
            question: q.question,
            group: q.group,
            score: q.score,
            status: true,
        })),
        jsonRandomRequest: null,
        createdAt: new Date(),
        siteId: +NEW_SITE_ID,
        oldId: examInMysql.id
    };

    await mongoDbCourse.collection('examSet').insertOne(newExamSet);
    await mongoDbExam.collection('roomTest').insertOne(newRoomTest);
    await mongoDbExam.collection('examSetRoomTest').insertOne(examSetRoomTest);
    return examSetRoomTest?._id ? newRoomTest?._id?.toString() : null
}

// Lấy dữ liệu theo từng batch
async function fetchBatch(sqlConnection, tableName, offset, limit, courseOldId) {
    const query = `
    SELECT * FROM ${tableName} 
    WHERE IDCourse IN (${courseOldId.filter(item => !!item).map(id => parseInt(id)).join(",")})
    AND IdParent IS NOT NULL 
    AND IsDeleted = 0
    LIMIT ${parseInt(limit)} 
    OFFSET ${parseInt(offset)}
    `;

    const [rows] = await sqlConnection.execute(query);

    console.log(`🟢 Lấy ${rows.length} bản ghi từ ${tableName} (Offset: ${offset})`);
    return rows;
}

async function findChapter(records, db) {
    if (records.length == 0) return [];
    const ids = [...new Set(records.map(item => item._id.toString()))];
    const rows = await db.collection("chapter").find({ courseId: { $in: ids } }).toArray();
    return rows;
}


function getType(row) {
    if (row.ExamType == 'room') return 5;
    if (row.ExamType == 'exercise') return 5;
    if (row.VideoFileType == "Video") return 0;
    if (row.VideoFileType == "Youtube") return 1;
    if (row.VideoFileType == "Scorm") return 3;

    return 4;

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
    if (row.ExamType == 'room' || row.ExamType == 'exercise') return 1

    return 2
}

async function findCourseBySite(db) {
    const rows = await db.collection("course").find({ siteId: +NEW_SITE_ID }).toArray();
    return rows;
}

async function migrateTable(sqlConnection, mongoDbCourse, mongoDbExam, tableName) {
    console.log(`🔄 Đang di chuyển bảng ${tableName}...`);


    const courseInMongo = await findCourseBySite(mongoDbCourse)

    const courseOldId = courseInMongo.map(item => item.oldId)

    const chapters = await findChapter(courseInMongo, mongoDbCourse);

    let offset = 0;
    while (true) {
        const rows = await fetchBatch(sqlConnection, tableName, offset, BATCH_SIZE, courseOldId);

        const RoomTestBatch = await findRoomTest(rows)

        const mappedLesson = [];

        for (const row of rows) {
            const roomTest = RoomTestBatch.find(item => row.IdExam == item.id);

            const chapter = chapters.find(item => row.IDParent == item.oldId);

            const idRoomExam = await findQuestionExam(roomTest, mongoDbCourse, mongoDbExam) || "";

            const file = row.FileUrls ? JSON.stringify(row.FileUrls) : null

            mappedLesson.push({
                chapterId: chapter?._id.toString(),
                courseId: chapter?.courseId.toString(),
                name: row.Name,
                lessonType: getType(row),
                urlLessonType: row.VideoFileUrl,
                uriVideo: row.VideoFileUrl,
                urlFileAttended: row.FileUrls?.[0]?.url || "",
                oldListFile: file,
                fileAttendedName: row.FileUrls ? JSON.stringify(row.FileUrls) : null,
                tag: [],
                status: 1,
                urlAvatar: "",
                description: row.Description || row.Content,
                lessonFinishStatus: getTypeEnd(row),
                percentFinish: 0,
                questions: [],
                score: 0,
                lessonStatus: 1,
                order: +row.DisplayOrder,
                testId: null,
                duration: 0,
                thumbnailUrl: row.ThumbnailFileUrl,
                numberQuestionPass: 0, //làm đúng bao nhiêu câu
                markPassExam: +roomTest?.pass_point || 0, // điểm pass bài
                createAt: row.CreatedDate,
                updateAt: row.ModifiedDate,
                roomTestId: idRoomExam,
                oldId: row.Id,
                oldIdExam: row.IdExam,
                siteId: +NEW_SITE_ID,
            });
        }


        // Kiểm tra nếu rows trống thì thoát khỏi vòng lặp
        if (!rows || rows.length === 0) break;




        await mongoDbCourse.collection('lesson').insertMany(mappedLesson);

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
    const mongoDbCourse = mongoClient.db(MONGO_DATABASE_COURSE);
    const mongoDbExam = mongoClient.db('db_moocs_exam');

    try {
        await migrateTable(sqlConnection, mongoDbCourse, mongoDbExam, 'CourseLesson');
    } catch (error) {
        console.error("❌ Lỗi khi di chuyển dữ liệu:", error);
    } finally {
        await sqlConnection.end();
        await mongoClient.close();
        console.log("🔚 Đã hoàn thành quá trình di chuyển.");
    }
}

migrate();