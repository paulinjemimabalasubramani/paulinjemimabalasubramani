import argparse


parser = argparse.ArgumentParser(description='Migrate any CSV type files with date info in file name')
parser.add_argument('--path', '--pth', '-p', help='Full path to the files', required=True)
parser.add_argument('--database', help='Database name in Snowflake', required=True)
parser.add_argument('--schema', help='Schema name in Snowflake', required=True)

args = parser.parse_args()

print(args.path)
print(args.database)
print(args.schema)


print(args.__dict__)




