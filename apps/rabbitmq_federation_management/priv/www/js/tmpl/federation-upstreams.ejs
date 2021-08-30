<h1>Federation Upstreams</h1>
<div class="section">
  <h2>Upstreams</h2>
  <div class="hider updatable">
<% if (upstreams.length > 0) { %>
<table class="list">
 <thead>
  <tr>
<% if (vhosts_interesting) { %>
    <th>Virtual Host</th>
<% } %>
    <th>Name</th>
    <th>URI</th>
    <th>Prefetch Count</th>
    <th>Reconnect Delay</th>
    <th>Ack mode</th>
    <th>Trust User-ID</th>
    <th>Exchange</th>
    <th>Max Hops</th>
    <th>Expiry</th>
    <th>Message TTL</th>
    <th>HA Policy</th>
    <th>Queue</th>
    <th>Consumer tag</th>
  </tr>
 </thead>
 <tbody>
<%
 for (var i = 0; i < upstreams.length; i++) {
    var upstream = upstreams[i];
%>
   <tr<%= alt_rows(i)%>>
<% if (vhosts_interesting) { %>
     <td><%= fmt_string(upstream.vhost) %></td>
<% } %>
     <td><%= link_fed_conn(upstream.vhost, upstream.name) %></td>
     <td><%= fmt_shortened_uri(fmt_uri_with_credentials(upstream.value.uri)) %></td>
     <td class="r"><%= upstream.value['prefetch-count'] %></td>
     <td class="r"><%= fmt_time(upstream.value['reconnect-delay'], 's') %></td>
     <td class="c"><%= fmt_string(upstream.value['ack-mode']) %></td>
     <td class="c"><%= fmt_boolean(upstream.value['trust-user-id']) %></td>
     <td class="c"><%= fmt_string(upstream.value['exchange']) %></td>
     <td class="r"><%= upstream.value['max-hops'] %></td>
     <td class="r"><%= fmt_time(upstream.value.expires, 'ms') %></td>
     <td class="r"><%= fmt_time(upstream.value['message-ttl'], 'ms') %></td>
     <td class="r"><%= fmt_string(upstream.value['ha-policy']) %></td>
     <td class="r"><%= fmt_string(upstream.value['queue']) %></td>
     <td class="r"><%= fmt_string(upstream.value['consumer-tag']) %></td>
   </tr>
<% } %>
 </tbody>
</table>
<% } else { %>
  <p>... no upstreams ...</p>
<% } %>
  </div>
</div>

<div class="section-hidden">
  <h2>Add a new upstream</h2>
  <div class="hider">
    <form action="#/fed-parameters" method="put">
      <input type="hidden" name="component" value="federation-upstream"/>
      <table class="form">
       <tr>
          <th>
          <h3>  General parameters </h3>
         </th>
        </tr>
<% if (vhosts_interesting) { %>
        <tr>
          <th><label>Virtual host:</label></th>
          <td>
            <select name="vhost">
              <% for (var i = 0; i < vhosts.length; i++) { %>
              <option value="<%= fmt_string(vhosts[i].name) %>"><%= fmt_string(vhosts[i].name) %></option>
              <% } %>
            </select>
          </td>
        </tr>
<% } else { %>
        <tr><td><input type="hidden" name="vhost" value="<%= fmt_string(vhosts[0].name) %>"/></td></tr>
<% } %>
        <tr>
          <th><label>Name:</label></th>
          <td><input type="text" name="name"/><span class="mand">*</span></td>
        </tr>


        <tr>
          <th>
            <label>
              URI:
              <span class="help" id="federation-uri"></span>
            </label>
          </th>
          <td><input type="text" name="uri"/><span class="mand">*</span></td>
        </tr>
        <tr>
          <th>
            <label>
              Prefetch count:
              <span class="help" id="federation-prefetch"></span>
            </label>
          </th>
          <td><input type="text" name="prefetch-count"/></td>
        </tr>
        <tr>
          <th>
            <label>
              Reconnect delay:
              <span class="help" id="federation-reconnect"></span>
            </label>
          </th>
          <td><input type="text" name="reconnect-delay"/> s</td>
        </tr>
        <tr>
          <th>
            <label>
              Acknowledgement Mode:
              <span class="help" id="federation-ack-mode"></span>
            </label>
          </th>
          <td>
            <select name="ack-mode">
              <option value="on-confirm">On confirm</option>
              <option value="on-publish">On publish</option>
              <option value="no-ack">No ack</option>
            </select>
          </td>
        </tr>
        <tr>
          <th>
            <label>
              Trust User-ID:
              <span class="help" id="federation-trust-user-id"></span>
            </label>
          </th>

          <td>
            <select name="trust-user-id">
              <option value="false">No</option>
              <option value="true">Yes</option>
            </select>
          </td>

        <tr>
          <th>
          <h3>Federated exchanges parameters </h3>
         </th>
        </tr>


         <tr>
          <th>
            <label>
              Exchange:
              <span class="help" id="exchange"></span>
            </label>
          </th>
          <td><input type="text" name="exchange"/></td>
        </tr>

       <tr>
          <th>
            <label>
              Max hops:
              <span class="help" id="federation-max-hops"></span>
            </label>
          </th>
          <td><input type="text" name="max-hops"/></td>
        </tr>

        <tr>
          <th>
            <label>
              Expires:
              <span class="help" id="federation-expires"></span>
            </label>
          </th>
          <td><input type="text" name="expires"/> ms</td>
        </tr>

        <tr>
          <th>
            <label>
              Message TTL:
              <span class="help" id="federation-ttl"></span>
            </label>
          </th>
          <td><input type="text" name="message-ttl"/> ms</td>
        </tr>


        <tr>
          <th>
            <label>
              HA Policy:
              <span class="help" id="ha-policy"></span>
            </label>
          </th>
          <td><input type="text" name="ha-policy"/></td>
        </tr>
        </tr>

       <tr>
          <th>
          <h3>Federated queues parameter </h3>
         </th>
        </tr>

         <tr>
          <th>
            <label>
              Queue:
              <span class="help" id="queue"></span>
            </label>
          </th>
          <td><input type="text" name="queue"/></td>
        </tr>
        <tr>
          <th>
            <label>
              Consumer tag:
              <span class="help" id="consumer-tag"></span>
            </label>
          </th>
          <td><input type="text" name="consumer-tag"/></td>
        </tr>
        </tr>



      </table>
      <input type="submit" value="Add upstream"/>
    </form>
  </div>
</div>
<div class="section-hidden">
  <h2>URI examples</h2>
  <div class="hider">
    <ul>
      <li>
        <code>amqp://server-name</code><br/>
        connect to server-name, without SSL and default credentials
      </li>
      <li>
        <code>amqp://user:password@server-name/my-vhost</code><br/>
        connect to server-name, with credentials and overridden
        virtual host
      </li>
      <li>
        <code>amqps://user:password@server-name?cacertfile=/path/to/cacert.pem&certfile=/path/to/cert.pem&keyfile=/path/to/key.pem&verify=verify_peer</code><br/>
        connect to server-name, with credentials and SSL
      </li>
      <li>
        <code>amqps://server-name?cacertfile=/path/to/cacert.pem&certfile=/path/to/cert.pem&keyfile=/path/to/key.pem&verify=verify_peer&fail_if_no_peer_cert=true&auth_mechanism=external</code><br/>
        connect to server-name, with SSL and EXTERNAL authentication
      </li>
    </ul>
  </div>
</div>
