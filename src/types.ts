
import { Document } from 'mongodb';

export type MongoTopicMessage<T> = {
    createdAt: Date
    logCompactId?: string
    payload: T
}
export type MongoTopicMessageDocument<T> = MongoTopicMessage<T> & {
    _id: number
}
export type SequenceDocument = Document & {
    seq: number
}