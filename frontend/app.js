'use strict';
const $ = id => document.getElementById(id);
const fmt = value => Number(value).toLocaleString('en-US');
let page = 1, pages = 1, requestId = 0;
async function api(path) {
  const response = await fetch(path, {headers: {'Accept': 'application/json'}});
  const data = await response.json();
  if (!response.ok) throw new Error(data.error || `HTTP ${response.status}`);
  return data;
}
function text(tag, value, className) {
  const node = document.createElement(tag); node.textContent = value;
  if (className) node.className = className;
  return node;
}
function showError(error) { $('error').textContent = error.message; $('error').hidden = false; }
async function loadUsers() {
  const current = ++requestId;
  $('error').hidden = true;
  const [sort, order] = $('sort').value.split(':');
  const params = new URLSearchParams({page, limit: 25, sort, order, q: $('search').value.trim()});
  try {
    const data = await api(`/api/users?${params}`);
    if (current !== requestId) return;
    $('rows').replaceChildren();
    for (const row of data.users) {
      const tr = document.createElement('tr');
      tr.append(text('td', row.user_id), text('td', fmt(row.followers)), text('td', fmt(row.followees)));
      $('rows').append(tr);
    }
    if (!data.users.length) { const tr = document.createElement('tr'); const td = text('td', 'No users match this search.'); td.colSpan = 3; tr.append(td); $('rows').append(tr); }
    pages = data.pages;
    $('page-label').textContent = `${fmt(data.total)} users · Page ${page} of ${Math.max(1, pages)}`;
    $('previous').disabled = page <= 1; $('next').disabled = page >= pages;
  } catch (error) { if (current === requestId) showError(error); }
}
async function init() {
  try {
    const [summary, required] = await Promise.all([api('/api/summary'), api('/api/users?ids=214328887,107830991')]);
    $('users').textContent = fmt(summary.unique_users); $('edges').textContent = fmt(summary.unique_edges);
    $('duplicates').textContent = fmt(summary.duplicate_edges_removed); $('loops').textContent = fmt(summary.self_loop_lines);
    $('source').textContent = summary.execution_mode === 'local' ? 'Showing local Spark test output.' : 'Showing output of the AWS Glue job, downloaded from S3.';
    $('engine').textContent = `Spark ${summary.spark_version}, ${summary.partitions} partitions`;
    for (const row of required.users) {
      const card = text('article', '', 'user-card');
      card.append(text('small', 'USER ID'), text('h3', row.user_id));
      for (const [field, label] of [['followers', 'followers'], ['followees', 'followees']]) {
        const p = document.createElement('p'); p.append(document.createTextNode(`${row.user_id} has `), text('strong', fmt(row[field])), document.createTextNode(` ${label}`)); card.append(p);
      }
      $('required').append(card);
    }
    if (required.missing.length) throw new Error('A required lab user is missing from the dataset.');
    await loadUsers();
  } catch (error) { $('source').textContent = 'Results could not be loaded.'; showError(error); }
}
$('search-form').addEventListener('submit', event => { event.preventDefault(); page = 1; loadUsers(); });
$('sort').addEventListener('change', () => { page = 1; loadUsers(); });
$('previous').addEventListener('click', () => { if (page > 1) { page--; loadUsers(); } });
$('next').addEventListener('click', () => { if (page < pages) { page++; loadUsers(); } });
init();
