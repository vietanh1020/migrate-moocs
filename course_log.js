import mysql from "mysql2/promise";
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
    MONGO_DATABASE_USER,
    IS_PROD = true
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

async function deleteOldData(db, table) {
    await db.collection(table).deleteMany({
        siteId: +NEW_SITE_ID,
        oldId: { $ne: null }  // Ensures oldId is not null
    });
}

// Lấy dữ liệu theo từng batch
async function fetchBatch(sqlConnection, tableName, offset, limit) {
    const query = `
    SELECT * FROM ${tableName} 
    WHERE IdSite = ${parseInt(OLD_SITE_ID)} 
    LIMIT ${parseInt(limit)} 
    OFFSET ${parseInt(offset)}
    `;

    const [rows] = await sqlConnection.execute(query);

    console.log(`🟢 Lấy ${rows.length} bản ghi từ ${tableName} (Offset: ${offset})`);
    return rows;
}

async function findCourses(mongoDbCourse) {
    const courses = await mongoDbCourse
        .collection("course")
        .find({ siteId: +NEW_SITE_ID })
        .toArray(); // Chuyển cursor thành mảng
    return courses;
}

async function findLesson(mongoDbCourse) {
    const courses = await mongoDbCourse
        .collection("lesson")
        .find({ siteId: +NEW_SITE_ID })
        .toArray(); // Chuyển cursor thành mảng
    return courses;
}

async function findUserByOld(id, dbUser) {
    const user = await dbUser.collection("user").findOne({ mobieduUserId: id });
    return user?._id?.toString() || "";
}

async function findAuthor(id, dbUser) {
    return await dbUser.collection("user").findOne(
        { _id: new ObjectId(id) },
    );
}


async function getCertDefault(mongoDbCourse) {
    const certDefault = await mongoDbCourse.collection("mCertificate").findOne({ siteId: +NEW_SITE_ID });
    if (certDefault) return certDefault;

    const newCert = {
        _id: new ObjectId(),
        name: "Chứng chỉ 1",
        url: !IS_PROD ? "https://media-moocs.mobifone.vn/moocs/images/5b1e2397-5c99-4f43-a163-d77d65790611.png" : "https://s3.pm-ptdv.com/moocs/images/0bd4c310-b798-4dfa-b486-9a2d511c32d1.png",
        siteId: +NEW_SITE_ID
    }

    await mongoDbCourse.collection("mCertificate").insertOne(newCert);
    return newCert
}

async function createClassRoom(mongoDbUser, courses, IDCERT) {
    const mappedClass = [];

    for (const course of courses) {

        const author = await findAuthor(course.authorId, mongoDbUser)

        const classroom = {
            _id: new ObjectId(),
            createdAt: course.createdAt,
            updatedAt: course.updatedAt,
            name: course.name,
            idTeachers: course.teacherIds,
            idCertificate: IDCERT,
            timeOpen: course.createdAt,
            timeClose: new Date("2035-01-01"),
            listIdsUser: [],
            listIdsGroup: [],
            listCourses: [
                {
                    idCourse: course._id.toString(),
                    name: course.name,
                    settingCourse: {
                        learnCourse: 1,
                        isPermitRegister: true,
                        listCourseAsignIds: [],
                        previousCourseIds: [],
                        openCourse: 1,
                        isRewind: true,
                        idCertificate: IDCERT,
                        progressDoneLesson: 0,
                        markPassLastExam: 0,
                        maximumCompletionTime: 0
                    },
                    certificate: null
                },
            ],
            documentsList: [],
            status: 1,
            userCreate: {
                _id: author?._id.toString(),
                name: author?.fullname,
                email: null,
                idUsers: null
            },
            isActive: true,
            siteId: +NEW_SITE_ID,
            completedPercent: 0,
            students: [],
            classType: 1,
            isHidden: false,
            linkMeeting: "",
            meetingRoomName: "",
            password: "",
            oldId: course.oldId
        };

        mappedClass.push(classroom)
    }

    if (mappedClass.length === 0) return [];

    return mappedClass
}

function createStudentClass(row, course, classRoom, userId, IDCERT) {
    const item = {
        _id: new ObjectId(),
        createdAt: new Date(row.CreatedDate),
        updatedAt: new Date(row.ModifiedDate),
        classId: classRoom._id.toString(),
        userId,
        listCourses: [
            {
                idCourse: course._id.toString(),
                name: course.name,
                settingCourse: {
                    learnCourse: 1,
                    isPermitRegister: true,
                    listCourseAsignIds: [],
                    previousCourseIds: [],
                    openCourse: 1,
                    isRewind: true,
                    idCertificate: IDCERT,
                    progressDoneLesson: 0,
                    markPassLastExam: 0,
                    maximumCompletionTime: 0
                },
                certificate: null
            }
        ],
        status: row.IsCompleted ? 3 : 2,
        completedPercent: Math.round((row.TotalCompletedLessons / row.TotalLessons) * 100),
        completedCourses: [],
        areLearningCourses: [],
        isActive: true,
        completedDate: new Date(row.ModifiedDate),
        siteId: +NEW_SITE_ID,
        oldId: row.Id
    };

    return item
}

function createStudentCourse(row, course, classRoom, userId) {
    const newsItem = {
        _id: new ObjectId(),
        createdAt: new Date(row.CreatedDate),
        updatedAt: new Date(row.ModifiedDate),
        courseId: course._id.toString(),
        classId: classRoom._id.toString(),
        userId: userId,
        teacherIds: course.teacherIds,
        status: row.IsCompleted ? 3 : 2,
        completedPercent: Math.round((row.TotalCompletedLessons / row.TotalLessons) * 100),
        completedDate: new Date(row.ModifiedDate),
        isRewind: true, // tua
        isOpenedAllLessons: true, // mở
        currentStudentLessonId: null, // bài 8 :))
        progressDoneLesson: Math.round((row.TotalCompletedLessons / row.TotalLessons) * 100),
        markPassLastExam: 0, // điểm hoàn thành
        maximumCompletionTime: 365 * 10,
        startedAt: new Date(row.CreatedDate),
        courseEndAt: new Date("2035-01-01"),
        siteId: +NEW_SITE_ID,
        oldIdUser: row.IdCreator,
        oldIdCourse: row.IdCourse,
        oldId: row.Id
    };

    return newsItem
}

async function createStudentLesson(studentCourse, allLesson, mongoDbCourse) {
    const courseId = studentCourse.courseId;

    const lessonCourse = allLesson.filter(item => item.courseId == courseId)

    const result = lessonCourse.map(lesson => {
        return {
            _id: new ObjectId(),
            createdAt: new Date(studentCourse.createdAt),
            updatedAt: new Date(studentCourse.updatedAt),
            classId: studentCourse.classId,
            lessonId: lesson._id.toString(),
            studentCourseId: studentCourse._id.toString(),
            courseId: lesson.courseId,
            chapterId: lesson.chapterId,
            order: lesson.order,
            userId: studentCourse.userId,
            status: 0,
            completedPercent: 0,
            isLocked: false,
            noteContent: null,
            currentTime: 0,
            questions: null,
            completedTime: 0,
            roomTestId: lesson.roomTestId,
            isPassRoomTest: false,
            oldId: -1,
            IdUser: studentCourse.oldIdUser,
            IdLesson: lesson.oldId,
            siteId: +NEW_SITE_ID
        }
    })

    await mongoDbCourse.collection('studentLesson').insertMany(result);
}

async function migrateTable(sqlConnection, mongoDbCourse, mongoDbClass, mongoDbUser, tableName) {
    console.log(`🔄 Đang di chuyển bảng ${tableName}...`);

    await deleteOldData(mongoDbCourse, "studentCourse")
    await deleteOldData(mongoDbCourse, "studentClass")
    await deleteOldData(mongoDbCourse, "studentLesson")
    await deleteOldData(mongoDbClass, "classRoom")

    const cert = await getCertDefault(mongoDbCourse)
    const IDCERT = cert?._id.toString()

    let offset = 0;

    const courses = await findCourses(mongoDbCourse);
    const allLesson = await findLesson(mongoDbCourse)

    // tạo lớp học 
    const classRooms = await createClassRoom(mongoDbUser, courses, IDCERT)

    const userInRoom = {}
    // hoàn thành khóa học
    while (true) {
        const rows = await fetchBatch(sqlConnection, tableName, offset, BATCH_SIZE);
        const mappedStudentCourse = [];
        const mappedStudentClass = [];

        for (const row of rows) {
            const course = courses.find(item => item.oldId == row.IdCourse)

            if (!course) continue


            const classRoom = classRooms.find(item => item.oldId == row.IdCourse)
            const classRoomId = classRoom?._id.toString();
            const userId = await findUserByOld(row.IdCreator, mongoDbUser)

            if (!userId) continue


            if (!userInRoom[classRoomId]) {
                userInRoom[classRoomId] = [];
            }
            if (!userInRoom[classRoomId].includes(userId)) {
                userInRoom[classRoomId].push(userId);
            }

            const newStudentCourse = createStudentCourse(row, course, classRoom, userId)
            const newStudentClass = createStudentClass(row, course, classRoom, userId, IDCERT)

            await createStudentLesson(newStudentCourse, allLesson, mongoDbCourse)

            mappedStudentCourse.push(newStudentCourse);
            mappedStudentClass.push(newStudentClass);
        }
        // Kiểm tra nếu rows trống thì thoát khỏi vòng lặp
        if (!rows || rows.length === 0) break;

        await mongoDbCourse.collection('studentCourse').insertMany(mappedStudentCourse);
        await mongoDbCourse.collection('studentClass').insertMany(mappedStudentClass);

        offset += +BATCH_SIZE;
    }

    const newClassRooms = classRooms.map((item) => ({
        ...item,
        students: userInRoom?.[item._id.toString()] || [],
        listIdsUser: userInRoom?.[item._id.toString()] || []
    }));
    await mongoDbClass.collection('classRoom').insertMany(newClassRooms);
}

const sqlConnection = await connectMySQL();

// Chạy quá trình di chuyển
async function migrate() {
    const mongoClient = new MongoClient(MONGO_URL);
    await mongoClient.connect();
    const mongoDb = mongoClient.db(MONGO_DATABASE_COURSE);
    const mongoDbClass = mongoClient.db('db_moocs_classes');
    const mongoDbUser = mongoClient.db(MONGO_DATABASE_USER)

    try {
        await migrateTable(sqlConnection, mongoDb, mongoDbClass, mongoDbUser, 'CompleteCourseLog');
    } catch (error) {
        console.error("❌ Lỗi khi di chuyển dữ liệu:", error);
    } finally {
        await sqlConnection.end();
        await mongoClient.close();
        console.log("🔚 Đã hoàn thành quá trình di chuyển.");
    }
}

migrate();