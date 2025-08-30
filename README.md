# My Own Simple Database

> "What I cannot create, I do not understand." – Richard Feynman

This project is an attempt to build a **toy relational database** from scratch in C.
As a web developer and student, databases felt like a **black box**—using them daily but not understanding their internals.
To truly grasp how they work, the goal was to **create one step by step**, uncovering the layers behind the scenes.[^1]

---

## 🚀 Motivation

I kept asking myself:

> _"As a web developer, I use relational databases every day at my job, but they’re a black box to me. Some questions I have:_
>
> - _What format is data saved in (in memory and on disk)?_
> - _When does it move from memory to disk?_
> - _Why can there only be one primary key per table?_
> - _How does rolling back a transaction work?_
> - _How are indexes formatted?_
> - _When and how does a full table scan happen?_
> - _What format is a prepared statement saved in?_ > _In other words, how does a database work?"_

Building this database helped answer many of these questions and illuminated the importance of hands-on persistence:

> _“Nothing in the world can take the place of persistence.” – Calvin Coolidge_

---

## 📚 What I Learned

By building this toy database, insights included:

- REPLs (**Read-Eval-Print Loops**) in C.[^1]
- How **data is stored on disk** in pages.
- Basic **B-Tree implementation** for indexing.
- Handling **SQL-like commands** (`insert`, `select`, `.btree`, `.exit`).
- Page size limitations and why real databases are more complex.
- The value of **understanding the internals** to become a better engineer.

---

## 🛠️ Setup \& Compilation

Make sure `gcc` is installed.

```bash
gcc repl.c -o repl
```

Run the database with a file to store your data:

```bash
./repl mydb.db
```

---

## 💻 Usage

Once running, you’ll see a prompt:

```
Sup boy>
```

#### ✅ Insert Data

```
insert 1 Pragun pragun@example.com
```

#### ✅ Select Data

```
select
```

_Output:_

```
(1, Pragun, pragun@example.com)
```

#### ✅ View the B-Tree

```
.btree
```

_Output:_

```
Tree:
- leaf (size 1)
  - 1
```

#### ✅ Exit the Database

```
.exit
```

---

## ⚠️ Limitations

- Table fills up after ~13 records (fixed page size).
- Only supports one table and very basic SQL.
- No transactions, rollbacks, or advanced indexing.
- This is a learning project, not production software.

---

## 🔮 Future Improvements

- Support for more rows (multiple pages).
- Support for multiple tables.
- Implement rollback/transactions.
- Add more SQL commands.

---

## 💡 Key Takeaway

Building even a tiny part of a database reveals how complex and beautiful these systems are.
This project is about persistence, curiosity, and breaking open the black box.

---

## 📎 Inspiration \& References

This project was inspired and guided by the excellent resources available at:

- [codecrafters-io/build-your-own-x](https://github.com/codecrafters-io/build-your-own-x) — Master programming by recreating your favorite technologies from scratch.[^1]

---

**Made with ❤️ by Pragun Kakkar**
**© 2025 Pragun Kakkar. All Rights Reserved.**

<div style="text-align: center">⁂</div>

[^1]: https://github.com/codecrafters-io/build-your-own-x
