
import { Document } from 'mongodb';

import { TopicMessage } from "../../types"

export type MongoTopicMessageDocument<T> = TopicMessage<T> & {
    _id: number
}
export type MongoSequenceDocument = Document & {
    seq: number
}