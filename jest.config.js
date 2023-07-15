/** @type {import('ts-jest').JestConfigWithTsJest} */
module.exports = {
  preset: 'ts-jest',
  testEnvironment: 'node',
  "verbose": true,
  "maxWorkers": 1,
  "forceExit": true,
  "testTimeout": 20000,
};